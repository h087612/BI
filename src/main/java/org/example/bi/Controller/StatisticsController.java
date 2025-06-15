package org.example.bi.Controller;

import jakarta.annotation.Resource;
import org.example.bi.DTO.StatisticsRequest;
import org.example.bi.DTO.StatisticsResponse;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * 新闻统计API控制器
 * 
 * 性能优化策略：
 * 1. 使用Redis缓存热点数据，避免频繁查询MySQL
 * 2. 利用索引优化SQL查询性能
 * 3. 使用并发查询减少响应时间
 * 4. 根据过滤条件智能选择查询策略
 */
@RestController
@RequestMapping("/news")
public class StatisticsController {
    
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    
    @Resource
    private JdbcTemplate jdbcTemplate;
    
    private final NamedParameterJdbcTemplate namedJdbcTemplate;
    private final ExecutorService executorService = Executors.newFixedThreadPool(10);
    
    // 数据日期范围常量
    private static final LocalDate DATA_MIN_DATE = LocalDate.parse("2019-06-13");
    private static final LocalDate DATA_MAX_DATE = LocalDate.parse("2019-07-12");
    
    // 参数限制常量
    private static final long TITLE_LENGTH_MIN = 0L;
    private static final long TITLE_LENGTH_MAX = 250L;
    private static final long CONTENT_LENGTH_MIN = 0L;
    private static final long CONTENT_LENGTH_MAX = 240000L;
    private static final int PAGE_SIZE_MAX = 100;
    
    // Lua脚本
    private final DefaultRedisScript<String> userFilteredStatisticsScript;
    
    public StatisticsController(JdbcTemplate jdbcTemplate) {
        this.namedJdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);
        
        // 初始化Lua脚本
        this.userFilteredStatisticsScript = new DefaultRedisScript<>();
        this.userFilteredStatisticsScript.setLocation(new ClassPathResource("user_filtered_statistics.lua"));
        this.userFilteredStatisticsScript.setResultType(String.class);
    }
    
    /**
     * 新闻统计查询API
     * 
     * 根据传入的过滤条件查询新闻点击统计数据
     * 支持多种过滤条件：类别、主题、标题长度、内容长度、用户偏好等
     * 
     * 参数处理说明：
     * - 字符串/数值参数：不传或传null都视为无过滤
     * - Boolean参数（like/dislike）：
     *   - 不传或传null：无过滤
     *   - 传false：无过滤（不是反向过滤）
     *   - 传true：启用相应过滤
     * 
     * @param request 查询参数
     * @return 统计结果，包含总点击量和分页的新闻数据
     */
    @GetMapping("/statistics")
    @Cacheable(value = "newsStatistics", 
               key = "#request.category + '_' + #request.topic + '_' + " +
                     "#request.titleLengthMin + '_' + #request.titleLengthMax + '_' + " +
                     "#request.contentLengthMin + '_' + #request.contentLengthMax + '_' + " +
                     "#request.startDate + '_' + #request.endDate + '_' + " +
                     "#request.like + '_' + #request.dislike",
               condition = "#request.userId == null && #request.userIds == null")
    public StatisticsResponse getStatistics(@ModelAttribute StatisticsRequest request) {
        long startTime = System.currentTimeMillis();
        StatisticsResponse response = new StatisticsResponse();
        
        try {
            // 1. 参数验证和预处理
            validateRequest(request);
            LocalDate startDate = validateStartDate(request.getStartDate());
            LocalDate endDate = validateEndDate(request.getEndDate());
            Set<String> userIdSet = parseUserIds(request);
            
            // 2. 判断查询策略
            boolean hasFilters = hasActiveFilters(request, userIdSet);
            
            // 3. 验证分页参数
            int page = request.getPage() != null ? request.getPage() : 1;
            int pageSize = request.getPageSize() != null ? request.getPageSize() : 20;
            
            // 4. 执行查询
            StatisticsResponse.StatisticsData data;
            if (hasFilters) {
                // 有过滤条件：使用混合查询
                data = executeFilteredQuery(startDate, endDate, request, userIdSet);
            } else {
                // 无过滤条件：使用Redis快速查询
                data = executeRedisQueryWithPaging(startDate, endDate, page, pageSize);
            }
            
            // 4. 构建响应
            response.setCode(200);
            response.setMessage("success");
            response.setData(data);
            response.setTimestamp(Instant.now().toEpochMilli());
            response.setElapsed(System.currentTimeMillis() - startTime);
            
        } catch (Exception e) {
            response.setCode(500);
            response.setMessage("Error: " + e.getMessage());
            response.setTimestamp(Instant.now().toEpochMilli());
            response.setElapsed(System.currentTimeMillis() - startTime);
            e.printStackTrace();
        }
        
        return response;
    }
    
    /**
     * 解析用户ID参数
     */
    private Set<String> parseUserIds(StatisticsRequest request) {
        Set<String> userIdSet = new HashSet<>();
        if (request.getUserId() != null && !request.getUserId().isEmpty()) {
            userIdSet.add(request.getUserId());
        }
        if (request.getUserIds() != null && !request.getUserIds().isEmpty()) {
            userIdSet.addAll(Arrays.asList(request.getUserIds().split(",")));
        }
        return userIdSet;
    }
    
    /**
     * 验证开始日期
     */
    private LocalDate validateStartDate(LocalDate startDate) {
        if (startDate == null) {
            return DATA_MAX_DATE.minusDays(2); // 默认最近3天
        }
        return startDate.isBefore(DATA_MIN_DATE) ? DATA_MIN_DATE : startDate;
    }
    
    /**
     * 验证结束日期
     */
    private LocalDate validateEndDate(LocalDate endDate) {
        if (endDate == null) {
            return DATA_MAX_DATE;
        }
        return endDate.isAfter(DATA_MAX_DATE) ? DATA_MAX_DATE : endDate;
    }
    
    /**
     * 验证请求参数
     * 
     * 说明：
     * - null值保持null（表示不过滤）
     * - 字符串"null"、"undefined"、空字符串都转换为真正的null
     * - 非null值会被限制在合理范围内
     * - 自动调整min/max顺序
     */
    private void validateRequest(StatisticsRequest request) {
        // 处理字符串"null"、"undefined"、空字符串的情况
        request.setCategory(normalizeStringParam(request.getCategory()));
        request.setTopic(normalizeStringParam(request.getTopic()));
        request.setUserId(normalizeStringParam(request.getUserId()));
        request.setUserIds(normalizeStringParam(request.getUserIds()));
        
        // 验证标题长度范围
        if (request.getTitleLengthMin() != null) {
            request.setTitleLengthMin(Math.max(TITLE_LENGTH_MIN, Math.min(TITLE_LENGTH_MAX, request.getTitleLengthMin())));
        }
        if (request.getTitleLengthMax() != null) {
            request.setTitleLengthMax(Math.max(TITLE_LENGTH_MIN, Math.min(TITLE_LENGTH_MAX, request.getTitleLengthMax())));
        }
        
        // 验证内容长度范围
        if (request.getContentLengthMin() != null) {
            request.setContentLengthMin(Math.max(CONTENT_LENGTH_MIN, Math.min(CONTENT_LENGTH_MAX, request.getContentLengthMin())));
        }
        if (request.getContentLengthMax() != null) {
            request.setContentLengthMax(Math.max(CONTENT_LENGTH_MIN, Math.min(CONTENT_LENGTH_MAX, request.getContentLengthMax())));
        }
        
        // 验证分页参数
        if (request.getPage() != null) {
            request.setPage(Math.max(1, request.getPage()));
        }
        if (request.getPageSize() != null) {
            request.setPageSize(Math.min(Math.max(1, request.getPageSize()), PAGE_SIZE_MAX));
        }
        
        // 确保 min <= max
        if (request.getTitleLengthMin() != null && request.getTitleLengthMax() != null) {
            if (request.getTitleLengthMin() > request.getTitleLengthMax()) {
                Long temp = request.getTitleLengthMin();
                request.setTitleLengthMin(request.getTitleLengthMax());
                request.setTitleLengthMax(temp);
            }
        }
        if (request.getContentLengthMin() != null && request.getContentLengthMax() != null) {
            if (request.getContentLengthMin() > request.getContentLengthMax()) {
                Long temp = request.getContentLengthMin();
                request.setContentLengthMin(request.getContentLengthMax());
                request.setContentLengthMax(temp);
            }
        }
    }
    
    /**
     * 标准化字符串参数
     * 将"null"、"undefined"、空字符串等转换为null
     */
    private String normalizeStringParam(String param) {
        if (param == null) {
            return null;
        }
        
        String trimmed = param.trim();
        if ("".equals(trimmed) || 
            "null".equalsIgnoreCase(trimmed) || 
            "undefined".equalsIgnoreCase(trimmed)) {
            return null;
        }
        
        return trimmed;
    }
    
    /**
     * 判断是否有过滤条件
     */
    private boolean hasActiveFilters(StatisticsRequest request, Set<String> userIdSet) {
        return request.getCategory() != null ||
               request.getTopic() != null || 
               request.getTitleLengthMin() != null || 
               request.getTitleLengthMax() != null ||
               request.getContentLengthMin() != null || 
               request.getContentLengthMax() != null ||
               request.getLike() != null ||
               request.getDislike() != null ||
               !userIdSet.isEmpty();
    }
    
    
    /**
     * Redis快速查询（支持分页）
     */
    private StatisticsResponse.StatisticsData executeRedisQueryWithPaging(LocalDate startDate, LocalDate endDate,
                                                                          int page, int pageSize) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        
        // 验证分页参数
        page = Math.max(1, page);
        pageSize = Math.min(Math.max(1, pageSize), 100); // 限制最大100条
        
        // 生成日期范围
        List<String> dateKeys = new ArrayList<>();
        LocalDate current = startDate;
        while (!current.isAfter(endDate)) {
            dateKeys.add(current.format(formatter));
            current = current.plusDays(1);
        }
        
        // 使用Pipeline批量获取Redis数据
        List<Object> results = stringRedisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            for (String dateKey : dateKeys) {
                String key = "news_hot_rank_daily:" + dateKey;
                // ZREVRANGE key 0 -1 WITHSCORES
                connection.zRevRangeWithScores(key.getBytes(), 0, -1);
            }
            return null;
        });
        
        // 聚合数据
        long totalClicks = 0;
        Map<String, Long> newsClickMap = new ConcurrentHashMap<>();
        
        for (Object result : results) {
            if (result instanceof Set) {
                Set<ZSetOperations.TypedTuple<String>> tuples = (Set<ZSetOperations.TypedTuple<String>>) result;
                for (ZSetOperations.TypedTuple<String> tuple : tuples) {
                    if (tuple.getValue() != null && tuple.getScore() != null) {
                        long clicks = tuple.getScore().longValue();
                        totalClicks += clicks;
                        newsClickMap.merge(tuple.getValue(), clicks, Long::sum);
                    }
                }
            }
        }
        
        // 排序所有新闻
        List<Map.Entry<String, Long>> sortedNews = newsClickMap.entrySet().stream()
            .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
            .collect(Collectors.toList());
        
        // 计算分页信息
        long totalNews = sortedNews.size();
        int totalPages = (int) Math.ceil((double) totalNews / pageSize);
        int startIndex = (page - 1) * pageSize;
        int endIndex = Math.min(startIndex + pageSize, sortedNews.size());
        
        // 获取当前页的数据
        List<StatisticsResponse.NewsStat> newsStats = new ArrayList<>();
        if (startIndex < sortedNews.size()) {
            newsStats = sortedNews.subList(startIndex, endIndex).stream()
                .map(entry -> new StatisticsResponse.NewsStat(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
        }
        
        // 构建返回数据
        StatisticsResponse.StatisticsData data = new StatisticsResponse.StatisticsData();
        data.setTotalClicks(totalClicks);
        data.setNewsStats(newsStats);
        data.setPage(page);
        data.setPageSize(pageSize);
        data.setTotalPages(totalPages);
        data.setTotalNews(totalNews);
        
        return data;
    }
    
    /**
     * 混合查询策略（有过滤条件）
     * 
     * 优化思路：
     * 1. 优先使用Redis数据，因为用户喜好数据和类别数据都在Redis中
     * 2. 只有当需要其他新闻属性过滤时才查询MySQL
     * 3. 尽可能避免纯MySQL查询
     */
    private StatisticsResponse.StatisticsData executeFilteredQuery(LocalDate startDate, LocalDate endDate,
                                                                   StatisticsRequest request, Set<String> userIdSet) {
        // 场景1：只有类别过滤（纯Redis - 使用类别日榜）
        if (request.getCategory() != null && !hasUserFilter(request, userIdSet) && 
            !hasLikeDislikeFilter(request) && !needsOtherNewsFilter(request)) {
            return executeRedisCategoryQuery(startDate, endDate, request);
        }
        
        // 场景2：只有用户ID/like/dislike过滤（纯Redis）
        if ((hasUserFilter(request, userIdSet) || hasLikeDislikeFilter(request)) && 
            !needsNewsFilter(request)) {
            return executeRedisUserFilterQuery(startDate, endDate, request, userIdSet);
        }
        
        // 场景3：类别 + 用户过滤（纯Redis）
        if (request.getCategory() != null && (hasUserFilter(request, userIdSet) || hasLikeDislikeFilter(request)) && 
            !needsOtherNewsFilter(request)) {
            return executeRedisCategoryUserQuery(startDate, endDate, request, userIdSet);
        }
        
        // 场景4：需要其他新闻属性过滤（Redis+MySQL混合）
        if (!hasUserFilter(request, userIdSet) && !hasLikeDislikeFilter(request) && needsNewsFilter(request)) {
            return executeHybridQuery(startDate, endDate, request);
        }
        
        // 场景5：用户过滤 + 新闻属性过滤（Redis+MySQL混合）
        if ((hasUserFilter(request, userIdSet) || hasLikeDislikeFilter(request)) && needsNewsFilter(request)) {
            return executeAdvancedHybridQuery(startDate, endDate, request, userIdSet);
        }
        
        // 场景6：无过滤条件（不应该到这里）
        return executeRedisQueryWithPaging(startDate, endDate, 
            request.getPage() != null ? request.getPage() : 1,
            request.getPageSize() != null ? request.getPageSize() : 20);
    }
    
    /**
     * Redis+MySQL混合查询
     */
    private StatisticsResponse.StatisticsData executeHybridQuery(LocalDate startDate, LocalDate endDate,
                                                                 StatisticsRequest request) {
        // 获取分页参数
        int page = request.getPage() != null ? request.getPage() : 1;
        int pageSize = request.getPageSize() != null ? request.getPageSize() : 20;
        page = Math.max(1, page);
        pageSize = Math.min(Math.max(1, pageSize), 100);
        
        // Step 1: 从Redis获取点击数据
        Map<String, Long> newsClickMap = getClickDataFromRedis(startDate, endDate);
        
        if (newsClickMap.isEmpty()) {
            return createEmptyResponse(page, pageSize);
        }
        
        // Step 2: 批量查询新闻属性
        Set<String> newsIds = newsClickMap.keySet();
        Map<String, NewsAttribute> newsAttributes = batchGetNewsAttributes(newsIds);
        
        // Step 3: 内存过滤
        Map<String, Long> filteredNewsMap = filterNewsByAttributes(newsClickMap, newsAttributes, request);
        
        // Step 4: 排序和分页
        List<Map.Entry<String, Long>> sortedNews = filteredNewsMap.entrySet().stream()
            .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
            .collect(Collectors.toList());
        
        // 计算分页信息
        long totalNews = sortedNews.size();
        long totalClicks = sortedNews.stream().mapToLong(Map.Entry::getValue).sum();
        int totalPages = (int) Math.ceil((double) totalNews / pageSize);
        int startIndex = (page - 1) * pageSize;
        int endIndex = Math.min(startIndex + pageSize, sortedNews.size());
        
        // 获取当前页数据
        List<StatisticsResponse.NewsStat> newsStats = new ArrayList<>();
        if (startIndex < sortedNews.size()) {
            newsStats = sortedNews.subList(startIndex, endIndex).stream()
                .map(entry -> new StatisticsResponse.NewsStat(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
        }
        
        StatisticsResponse.StatisticsData data = new StatisticsResponse.StatisticsData();
        data.setTotalClicks(totalClicks);
        data.setNewsStats(newsStats);
        data.setPage(page);
        data.setPageSize(pageSize);
        data.setTotalPages(totalPages);
        data.setTotalNews(totalNews);
        
        return data;
    }
    
    /**
     * 纯MySQL查询（用于有用户过滤的情况）
     */
    private StatisticsResponse.StatisticsData executePureMySQLQuery(LocalDate startDate, LocalDate endDate,
                                                                    StatisticsRequest request, Set<String> userIdSet) {
        // 获取分页参数
        int page = request.getPage() != null ? request.getPage() : 1;
        int pageSize = request.getPageSize() != null ? request.getPageSize() : 20;
        
        // 并发执行三个查询
        CompletableFuture<Long> totalClicksFuture = CompletableFuture.supplyAsync(() -> 
            calculateTotalClicks(startDate, endDate, request, userIdSet), executorService);
        
        CompletableFuture<List<StatisticsResponse.NewsStat>> newsStatsFuture = CompletableFuture.supplyAsync(() -> 
            getNewsStats(startDate, endDate, request, userIdSet), executorService);
        
        CompletableFuture<Long> totalNewsFuture = CompletableFuture.supplyAsync(() -> 
            calculateTotalNews(startDate, endDate, request, userIdSet), executorService);
        
        // 等待查询完成
        CompletableFuture.allOf(totalClicksFuture, newsStatsFuture, totalNewsFuture).join();
        
        // 构建返回数据
        long totalNews = totalNewsFuture.join();
        int totalPages = (int) Math.ceil((double) totalNews / pageSize);
        
        StatisticsResponse.StatisticsData data = new StatisticsResponse.StatisticsData();
        data.setTotalClicks(totalClicksFuture.join());
        data.setNewsStats(newsStatsFuture.join());
        data.setPage(page);
        data.setPageSize(pageSize);
        data.setTotalPages(totalPages);
        data.setTotalNews(totalNews);
        
        return data;
    }
    
    /**
     * 计算总点击量
     * 
     * 优化策略：
     * 1. 使用EXISTS代替JOIN减少开销
     * 2. 利用索引加速查询
     */
    private long calculateTotalClicks(LocalDate startDate, LocalDate endDate,
                                     StatisticsRequest request, Set<String> userIdSet) {
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("startTime", startDate.atStartOfDay());
        params.addValue("endTime", endDate.atTime(23, 59, 59));
        
        // 基础查询
        StringBuilder sql = new StringBuilder(
            "SELECT COUNT(*) FROM user_clicklog uc " +
            "WHERE uc.click_time BETWEEN :startTime AND :endTime "
        );
        
        // 处理like/dislike条件
        if (Boolean.TRUE.equals(request.getLike())) {
            sql.append("AND uc.pos_or_neg = 1 ");
        } else if (Boolean.TRUE.equals(request.getDislike())) {
            sql.append("AND uc.pos_or_neg = 0 ");
        }
        
        // 处理用户ID过滤
        if (!userIdSet.isEmpty()) {
            sql.append("AND uc.user_id IN (:userIds) ");
            params.addValue("userIds", userIdSet);
        }
        
        // 处理新闻属性过滤（使用EXISTS优化）
        if (needsNewsFilter(request)) {
            sql.append("AND EXISTS (SELECT 1 FROM static_news n WHERE n.news_id = uc.clicknews_id ");
            addNewsFilters(sql, request, params);
            sql.append(") ");
        }
        
        Long count = namedJdbcTemplate.queryForObject(sql.toString(), params, Long.class);
        return count != null ? count : 0L;
    }
    
    /**
     * 获取新闻点击统计（支持分页）
     * 
     * 优化策略：
     * 1. 强制使用合适的索引
     * 2. 减少不必要的JOIN
     */
    private List<StatisticsResponse.NewsStat> getNewsStats(LocalDate startDate, LocalDate endDate,
                                                           StatisticsRequest request, Set<String> userIdSet) {
        // 获取分页参数
        int page = request.getPage() != null ? request.getPage() : 1;
        int pageSize = request.getPageSize() != null ? request.getPageSize() : 20;
        page = Math.max(1, page);
        pageSize = Math.min(Math.max(1, pageSize), 100);
        
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("startTime", startDate.atStartOfDay());
        params.addValue("endTime", endDate.atTime(23, 59, 59));
        
        // 构建查询
        StringBuilder sql = new StringBuilder(
            "SELECT uc.clicknews_id as newsId, COUNT(*) as clickCount " +
            "FROM user_clicklog uc "
        );
        
        // 只在需要时JOIN新闻表
        if (needsNewsFilter(request)) {
            sql.append("JOIN static_news n ON uc.clicknews_id = n.news_id ");
        }
        
        sql.append("WHERE uc.click_time BETWEEN :startTime AND :endTime ");
        
        // 处理like/dislike条件
        if (Boolean.TRUE.equals(request.getLike())) {
            sql.append("AND uc.pos_or_neg = 1 ");
        } else if (Boolean.TRUE.equals(request.getDislike())) {
            sql.append("AND uc.pos_or_neg = 0 ");
        }
        
        // 处理用户ID过滤
        if (!userIdSet.isEmpty()) {
            sql.append("AND uc.user_id IN (:userIds) ");
            params.addValue("userIds", userIdSet);
        }
        
        // 添加新闻过滤条件
        if (needsNewsFilter(request)) {
            addNewsFilters(sql, request, params);
        }
        
        // 分组并排序，使用分页
        int offset = (page - 1) * pageSize;
        sql.append("GROUP BY uc.clicknews_id ORDER BY clickCount DESC LIMIT :limit OFFSET :offset");
        params.addValue("limit", pageSize);
        params.addValue("offset", offset);
        
        return namedJdbcTemplate.query(sql.toString(), params, (rs, rowNum) -> 
            new StatisticsResponse.NewsStat(rs.getString("newsId"), rs.getLong("clickCount")));
    }
    
    /**
     * 计算符合条件的新闻总数（用于分页）
     */
    private long calculateTotalNews(LocalDate startDate, LocalDate endDate,
                                   StatisticsRequest request, Set<String> userIdSet) {
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("startTime", startDate.atStartOfDay());
        params.addValue("endTime", endDate.atTime(23, 59, 59));
        
        // 构建查询
        StringBuilder sql = new StringBuilder(
            "SELECT COUNT(DISTINCT uc.clicknews_id) " +
            "FROM user_clicklog uc "
        );
        
        // 只在需要时JOIN新闻表
        if (needsNewsFilter(request)) {
            sql.append("JOIN static_news n ON uc.clicknews_id = n.news_id ");
        }
        
        sql.append("WHERE uc.click_time BETWEEN :startTime AND :endTime ");
        
        // 处理like/dislike条件
        if (Boolean.TRUE.equals(request.getLike())) {
            sql.append("AND uc.pos_or_neg = 1 ");
        } else if (Boolean.TRUE.equals(request.getDislike())) {
            sql.append("AND uc.pos_or_neg = 0 ");
        }
        
        // 处理用户ID过滤
        if (!userIdSet.isEmpty()) {
            sql.append("AND uc.user_id IN (:userIds) ");
            params.addValue("userIds", userIdSet);
        }
        
        // 添加新闻过滤条件
        if (needsNewsFilter(request)) {
            addNewsFilters(sql, request, params);
        }
        
        Long count = namedJdbcTemplate.queryForObject(sql.toString(), params, Long.class);
        return count != null ? count : 0L;
    }
    
    /**
     * 判断是否需要新闻表过滤
     */
    private boolean needsNewsFilter(StatisticsRequest request) {
        return request.getCategory() != null ||
               request.getTopic() != null || 
               request.getTitleLengthMin() != null || 
               request.getTitleLengthMax() != null ||
               request.getContentLengthMin() != null || 
               request.getContentLengthMax() != null;
    }
    
    /**
     * 判断是否需要其他新闻属性过滤（排除category）
     */
    private boolean needsOtherNewsFilter(StatisticsRequest request) {
        return request.getTopic() != null || 
               request.getTitleLengthMin() != null || 
               request.getTitleLengthMax() != null ||
               request.getContentLengthMin() != null || 
               request.getContentLengthMax() != null;
    }
    
    /**
     * 添加新闻过滤条件
     */
    private void addNewsFilters(StringBuilder sql, StatisticsRequest request, MapSqlParameterSource params) {
        if (request.getCategory() != null) {
            sql.append("AND n.category = :category ");
            params.addValue("category", request.getCategory());
        }
        if (request.getTopic() != null) {
            sql.append("AND n.topic = :topic ");
            params.addValue("topic", request.getTopic());
        }
        if (request.getTitleLengthMin() != null) {
            sql.append("AND n.headline_length >= :titleMin ");
            params.addValue("titleMin", request.getTitleLengthMin());
        }
        if (request.getTitleLengthMax() != null) {
            sql.append("AND n.headline_length <= :titleMax ");
            params.addValue("titleMax", request.getTitleLengthMax());
        }
        if (request.getContentLengthMin() != null) {
            sql.append("AND n.body_length >= :contentMin ");
            params.addValue("contentMin", request.getContentLengthMin());
        }
        if (request.getContentLengthMax() != null) {
            sql.append("AND n.body_length <= :contentMax ");
            params.addValue("contentMax", request.getContentLengthMax());
        }
    }
    
    /**
     * 判断是否有用户过滤条件
     */
    private boolean hasUserFilter(StatisticsRequest request, Set<String> userIdSet) {
        return !userIdSet.isEmpty();
    }
    
    /**
     * 判断是否有like/dislike过滤条件
     * 注意：只有明确设置为true时才认为有过滤条件
     */
    private boolean hasLikeDislikeFilter(StatisticsRequest request) {
        return Boolean.TRUE.equals(request.getLike()) || Boolean.TRUE.equals(request.getDislike());
    }
    
    /**
     * 从Redis获取点击数据
     */
    private Map<String, Long> getClickDataFromRedis(LocalDate startDate, LocalDate endDate) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        Map<String, Long> newsClickMap = new ConcurrentHashMap<>();
        
        // 生成日期范围
        List<String> dateKeys = new ArrayList<>();
        LocalDate current = startDate;
        while (!current.isAfter(endDate)) {
            dateKeys.add(current.format(formatter));
            current = current.plusDays(1);
        }
        
        // Pipeline批量获取
        List<Object> results = stringRedisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            for (String dateKey : dateKeys) {
                String key = "news_hot_rank_daily:" + dateKey;
                connection.zRevRangeWithScores(key.getBytes(), 0, -1);
            }
            return null;
        });
        
        // 聚合数据
        for (Object result : results) {
            if (result instanceof Set) {
                Set<ZSetOperations.TypedTuple<String>> tuples = (Set<ZSetOperations.TypedTuple<String>>) result;
                for (ZSetOperations.TypedTuple<String> tuple : tuples) {
                    if (tuple.getValue() != null && tuple.getScore() != null) {
                        newsClickMap.merge(tuple.getValue(), tuple.getScore().longValue(), Long::sum);
                    }
                }
            }
        }
        
        return newsClickMap;
    }
    
    /**
     * 批量获取新闻属性
     */
    private Map<String, NewsAttribute> batchGetNewsAttributes(Set<String> newsIds) {
        if (newsIds.isEmpty()) {
            return new HashMap<>();
        }
        
        // 分批查询，避免IN子句过长
        Map<String, NewsAttribute> result = new HashMap<>();
        List<String> idList = new ArrayList<>(newsIds);
        int batchSize = 1000;
        
        for (int i = 0; i < idList.size(); i += batchSize) {
            List<String> batch = idList.subList(i, Math.min(i + batchSize, idList.size()));
            
            String sql = "SELECT news_id, category, topic, headline_length, body_length " +
                        "FROM static_news WHERE news_id IN (:newsIds)";
            
            MapSqlParameterSource params = new MapSqlParameterSource();
            params.addValue("newsIds", batch);
            
            List<NewsAttribute> attributes = namedJdbcTemplate.query(sql, params, (rs, rowNum) -> {
                NewsAttribute attr = new NewsAttribute();
                attr.setNewsId(rs.getString("news_id"));
                attr.setCategory(rs.getString("category"));
                attr.setTopic(rs.getString("topic"));
                attr.setHeadlineLength(rs.getInt("headline_length"));
                attr.setBodyLength(rs.getInt("body_length"));
                return attr;
            });
            
            for (NewsAttribute attr : attributes) {
                result.put(attr.getNewsId(), attr);
            }
        }
        
        return result;
    }
    
    /**
     * 根据属性过滤新闻
     */
    private Map<String, Long> filterNewsByAttributes(Map<String, Long> newsClickMap,
                                                     Map<String, NewsAttribute> newsAttributes,
                                                     StatisticsRequest request) {
        return newsClickMap.entrySet().stream()
            .filter(entry -> {
                NewsAttribute attr = newsAttributes.get(entry.getKey());
                if (attr == null) {
                    return false;
                }
                
                // 类别过滤
                if (request.getCategory() != null && !request.getCategory().equals(attr.getCategory())) {
                    return false;
                }
                
                // 主题过滤
                if (request.getTopic() != null && !request.getTopic().equals(attr.getTopic())) {
                    return false;
                }
                
                // 标题长度过滤
                if (request.getTitleLengthMin() != null && attr.getHeadlineLength() < request.getTitleLengthMin()) {
                    return false;
                }
                if (request.getTitleLengthMax() != null && attr.getHeadlineLength() > request.getTitleLengthMax()) {
                    return false;
                }
                
                // 内容长度过滤
                if (request.getContentLengthMin() != null && attr.getBodyLength() < request.getContentLengthMin()) {
                    return false;
                }
                if (request.getContentLengthMax() != null && attr.getBodyLength() > request.getContentLengthMax()) {
                    return false;
                }
                
                return true;
            })
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
    
    /**
     * 创建空响应
     */
    private StatisticsResponse.StatisticsData createEmptyResponse() {
        return createEmptyResponse(1, 20);
    }
    
    /**
     * 创建空响应（带分页）
     */
    private StatisticsResponse.StatisticsData createEmptyResponse(int page, int pageSize) {
        StatisticsResponse.StatisticsData data = new StatisticsResponse.StatisticsData();
        data.setTotalClicks(0L);
        data.setNewsStats(new ArrayList<>());
        data.setPage(page);
        data.setPageSize(pageSize);
        data.setTotalPages(0);
        data.setTotalNews(0L);
        return data;
    }
    
    /**
     * 使用Lua脚本执行Redis查询（用户喜好过滤）
     */
    private StatisticsResponse.StatisticsData executeRedisLuaQuery(LocalDate startDate, LocalDate endDate,
                                                                   String userId, StatisticsRequest request) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        
        // 准备Lua脚本参数
        List<String> keys = Arrays.asList(
            startDate.format(formatter),  // 开始日期
            endDate.format(formatter),    // 结束日期
            userId                        // 用户ID
        );
        
        // 确定过滤类型
        String filterType = "all";
        if (Boolean.TRUE.equals(request.getLike())) {
            filterType = "like";
        } else if (Boolean.TRUE.equals(request.getDislike())) {
            filterType = "dislike";
        }
        
        // 执行Lua脚本
        String result = stringRedisTemplate.execute(
            userFilteredStatisticsScript,
            keys,
            "20",       // 限制数量
            filterType  // 过滤类型
        );
        
        // 解析JSON结果
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(result);
            
            StatisticsResponse.StatisticsData data = new StatisticsResponse.StatisticsData();
            data.setTotalClicks(root.get("totalClicks").asLong());
            
            List<StatisticsResponse.NewsStat> newsStats = new ArrayList<>();
            JsonNode statsArray = root.get("newsStats");
            if (statsArray != null && statsArray.isArray()) {
                for (JsonNode stat : statsArray) {
                    newsStats.add(new StatisticsResponse.NewsStat(
                        stat.get("newsId").asText(),
                        stat.get("clickCount").asLong()
                    ));
                }
            }
            data.setNewsStats(newsStats);
            
            return data;
        } catch (Exception e) {
            // 如果Lua脚本执行失败，回退到纯MySQL查询
            Set<String> userIdSet = new HashSet<>();
            userIdSet.add(userId);
            return executePureMySQLQuery(startDate, endDate, request, userIdSet);
        }
    }
    
    /**
     * 新闻属性内部类
     */
    private static class NewsAttribute {
        private String newsId;
        private String category;
        private String topic;
        private int headlineLength;
        private int bodyLength;
        
        // Getters and Setters
        public String getNewsId() { return newsId; }
        public void setNewsId(String newsId) { this.newsId = newsId; }
        
        public String getCategory() { return category; }
        public void setCategory(String category) { this.category = category; }
        
        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }
        
        public int getHeadlineLength() { return headlineLength; }
        public void setHeadlineLength(int headlineLength) { this.headlineLength = headlineLength; }
        
        public int getBodyLength() { return bodyLength; }
        public void setBodyLength(int bodyLength) { this.bodyLength = bodyLength; }
    }
    
    /**
     * 纯Redis用户过滤查询
     * 利用Redis中的user_seen_news和user_dislike_news集合
     */
    private StatisticsResponse.StatisticsData executeRedisUserFilterQuery(LocalDate startDate, LocalDate endDate,
                                                                          StatisticsRequest request, Set<String> userIdSet) {
        // 获取分页参数
        int page = request.getPage() != null ? request.getPage() : 1;
        int pageSize = request.getPageSize() != null ? request.getPageSize() : 20;
        
        // Step 1: 从Redis获取日榜数据
        Map<String, Long> allNewsClicks = getClickDataFromRedis(startDate, endDate);
        if (allNewsClicks.isEmpty()) {
            return createEmptyResponse(page, pageSize);
        }
        
        // Step 2: 获取用户过滤的新闻集合
        Set<String> filteredNewsIds = getUserFilteredNewsFromRedis(request, userIdSet);
        
        // Step 3: 过滤并聚合
        Map<String, Long> filteredClicks = new HashMap<>();
        long totalClicks = 0;
        
        for (Map.Entry<String, Long> entry : allNewsClicks.entrySet()) {
            if (filteredNewsIds.contains(entry.getKey())) {
                filteredClicks.put(entry.getKey(), entry.getValue());
                totalClicks += entry.getValue();
            }
        }
        
        // Step 4: 排序和分页
        return buildPagedResponse(filteredClicks, totalClicks, page, pageSize);
    }
    
    /**
     * 高级混合查询（用户过滤 + 新闻属性过滤）
     */
    private StatisticsResponse.StatisticsData executeAdvancedHybridQuery(LocalDate startDate, LocalDate endDate,
                                                                         StatisticsRequest request, Set<String> userIdSet) {
        // 获取分页参数
        int page = request.getPage() != null ? request.getPage() : 1;
        int pageSize = request.getPageSize() != null ? request.getPageSize() : 20;
        
        // Step 1: 从Redis获取日榜数据
        Map<String, Long> allNewsClicks = getClickDataFromRedis(startDate, endDate);
        if (allNewsClicks.isEmpty()) {
            return createEmptyResponse(page, pageSize);
        }
        
        // Step 2: Redis过滤 - 获取用户过滤的新闻
        Set<String> userFilteredNews = getUserFilteredNewsFromRedis(request, userIdSet);
        
        // Step 3: 初步过滤，减少需要查询MySQL的数据量
        Map<String, Long> userFilteredClicks = new HashMap<>();
        for (Map.Entry<String, Long> entry : allNewsClicks.entrySet()) {
            if (userFilteredNews.contains(entry.getKey())) {
                userFilteredClicks.put(entry.getKey(), entry.getValue());
            }
        }
        
        // Step 4: MySQL过滤 - 批量查询新闻属性并过滤
        Map<String, NewsAttribute> newsAttributes = batchGetNewsAttributes(userFilteredClicks.keySet());
        Map<String, Long> finalFilteredClicks = filterNewsByAttributes(userFilteredClicks, newsAttributes, request);
        
        // Step 5: 计算总点击量
        long totalClicks = finalFilteredClicks.values().stream().mapToLong(Long::longValue).sum();
        
        // Step 6: 返回分页结果
        return buildPagedResponse(finalFilteredClicks, totalClicks, page, pageSize);
    }
    
    /**
     * 从Redis获取用户过滤的新闻集合
     */
    private Set<String> getUserFilteredNewsFromRedis(StatisticsRequest request, Set<String> userIdSet) {
        Set<String> result = new HashSet<>();
        
        // 如果没有用户过滤，返回空集合（表示不过滤）
        if (userIdSet.isEmpty() && !hasLikeDislikeFilter(request)) {
            return result;
        }
        
        // 处理每个用户
        for (String userId : userIdSet) {
            Set<String> userNews = null;
            
            if (Boolean.TRUE.equals(request.getLike())) {
                // 获取用户喜欢的新闻（seen集合）
                userNews = stringRedisTemplate.opsForSet().members("user_seen_news:" + userId);
            } else if (Boolean.TRUE.equals(request.getDislike())) {
                // 获取用户不喜欢的新闻（dislike集合）
                userNews = stringRedisTemplate.opsForSet().members("user_dislike_news:" + userId);
            } else {
                // 既不是like也不是dislike，获取所有看过的新闻（seen + dislike）
                Set<String> seen = stringRedisTemplate.opsForSet().members("user_seen_news:" + userId);
                Set<String> dislike = stringRedisTemplate.opsForSet().members("user_dislike_news:" + userId);
                userNews = new HashSet<>();
                if (seen != null) userNews.addAll(seen);
                if (dislike != null) userNews.addAll(dislike);
            }
            
            if (userNews != null) {
                if (result.isEmpty()) {
                    result.addAll(userNews);
                } else {
                    // 如果有多个用户，取并集
                    result.addAll(userNews);
                }
            }
        }
        
        return result;
    }
    
    /**
     * 构建分页响应
     */
    private StatisticsResponse.StatisticsData buildPagedResponse(Map<String, Long> newsClicks, 
                                                                 long totalClicks, int page, int pageSize) {
        // 排序
        List<Map.Entry<String, Long>> sortedNews = newsClicks.entrySet().stream()
            .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
            .collect(Collectors.toList());
        
        // 计算分页
        long totalNews = sortedNews.size();
        int totalPages = (int) Math.ceil((double) totalNews / pageSize);
        int startIndex = (page - 1) * pageSize;
        int endIndex = Math.min(startIndex + pageSize, sortedNews.size());
        
        // 获取当前页数据
        List<StatisticsResponse.NewsStat> newsStats = new ArrayList<>();
        if (startIndex < sortedNews.size()) {
            newsStats = sortedNews.subList(startIndex, endIndex).stream()
                .map(entry -> new StatisticsResponse.NewsStat(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
        }
        
        // 构建响应
        StatisticsResponse.StatisticsData data = new StatisticsResponse.StatisticsData();
        data.setTotalClicks(totalClicks);
        data.setNewsStats(newsStats);
        data.setPage(page);
        data.setPageSize(pageSize);
        data.setTotalPages(totalPages);
        data.setTotalNews(totalNews);
        
        return data;
    }
    
    /**
     * 纯Redis类别查询
     * 使用news_hot_rank_daily:{category}:{date}数据
     */
    private StatisticsResponse.StatisticsData executeRedisCategoryQuery(LocalDate startDate, LocalDate endDate,
                                                                        StatisticsRequest request) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        String category = request.getCategory();
        int page = request.getPage() != null ? request.getPage() : 1;
        int pageSize = request.getPageSize() != null ? request.getPageSize() : 20;
        
        // 生成日期范围
        List<String> dateKeys = new ArrayList<>();
        LocalDate current = startDate;
        while (!current.isAfter(endDate)) {
            dateKeys.add(current.format(formatter));
            current = current.plusDays(1);
        }
        
        // 使用Pipeline批量获取类别日榜数据
        List<Object> results = stringRedisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            for (String dateKey : dateKeys) {
                // 使用类别日榜key
                String key = "news_hot_rank_daily:" + category + ":" + dateKey;
                connection.zRevRangeWithScores(key.getBytes(), 0, -1);
            }
            return null;
        });
        
        // 聚合数据
        Map<String, Long> newsClickMap = new ConcurrentHashMap<>();
        long totalClicks = 0;
        
        for (Object result : results) {
            if (result instanceof Set) {
                Set<ZSetOperations.TypedTuple<String>> tuples = (Set<ZSetOperations.TypedTuple<String>>) result;
                for (ZSetOperations.TypedTuple<String> tuple : tuples) {
                    if (tuple.getValue() != null && tuple.getScore() != null) {
                        long clicks = tuple.getScore().longValue();
                        totalClicks += clicks;
                        newsClickMap.merge(tuple.getValue(), clicks, Long::sum);
                    }
                }
            }
        }
        
        return buildPagedResponse(newsClickMap, totalClicks, page, pageSize);
    }
    
    /**
     * Redis类别+用户过滤查询
     * 结合类别日榜和用户喜好数据
     */
    private StatisticsResponse.StatisticsData executeRedisCategoryUserQuery(LocalDate startDate, LocalDate endDate,
                                                                            StatisticsRequest request, Set<String> userIdSet) {
        // Step 1: 获取类别过滤的新闻数据
        Map<String, Long> categoryNewsClicks = getCategoryClickDataFromRedis(startDate, endDate, request.getCategory());
        
        if (categoryNewsClicks.isEmpty()) {
            return createEmptyResponse(request.getPage() != null ? request.getPage() : 1,
                                     request.getPageSize() != null ? request.getPageSize() : 20);
        }
        
        // Step 2: 获取用户过滤的新闻集合
        Set<String> userFilteredNews = getUserFilteredNewsFromRedis(request, userIdSet);
        
        // Step 3: 取交集
        Map<String, Long> filteredClicks = new HashMap<>();
        long totalClicks = 0;
        
        for (Map.Entry<String, Long> entry : categoryNewsClicks.entrySet()) {
            if (userFilteredNews.contains(entry.getKey())) {
                filteredClicks.put(entry.getKey(), entry.getValue());
                totalClicks += entry.getValue();
            }
        }
        
        // Step 4: 返回分页结果
        return buildPagedResponse(filteredClicks, totalClicks,
            request.getPage() != null ? request.getPage() : 1,
            request.getPageSize() != null ? request.getPageSize() : 20);
    }
    
    /**
     * 从Redis获取类别点击数据
     */
    private Map<String, Long> getCategoryClickDataFromRedis(LocalDate startDate, LocalDate endDate, String category) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        Map<String, Long> newsClickMap = new ConcurrentHashMap<>();
        
        // 生成日期范围
        List<String> dateKeys = new ArrayList<>();
        LocalDate current = startDate;
        while (!current.isAfter(endDate)) {
            dateKeys.add(current.format(formatter));
            current = current.plusDays(1);
        }
        
        // Pipeline批量获取
        List<Object> results = stringRedisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            for (String dateKey : dateKeys) {
                String key = "news_hot_rank_daily:" + category + ":" + dateKey;
                connection.zRevRangeWithScores(key.getBytes(), 0, -1);
            }
            return null;
        });
        
        // 聚合数据
        for (Object result : results) {
            if (result instanceof Set) {
                Set<ZSetOperations.TypedTuple<String>> tuples = (Set<ZSetOperations.TypedTuple<String>>) result;
                for (ZSetOperations.TypedTuple<String> tuple : tuples) {
                    if (tuple.getValue() != null && tuple.getScore() != null) {
                        newsClickMap.merge(tuple.getValue(), tuple.getScore().longValue(), Long::sum);
                    }
                }
            }
        }
        
        return newsClickMap;
    }
}