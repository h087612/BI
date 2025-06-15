package org.example.bi.Controller;

import jakarta.annotation.Resource;
import org.example.bi.Entity.News;
import org.example.bi.Repository.NewsRepository;
import org.example.bi.Repository.UserClickRepository;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/news")
public class StatisticsController {
    
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    
    @Resource
    private NewsRepository newsRepository;
    
    @Resource
    private JdbcTemplate jdbcTemplate;
    
    @GetMapping("/statistics")
    public Map<String, Object> getStatistics(
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDate startDate,
            @RequestParam(required = false) @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDate endDate,
            @RequestParam(required = false) String topic,
            @RequestParam(required = false) Integer titleLengthMin,
            @RequestParam(required = false) Integer titleLengthMax,
            @RequestParam(required = false) Integer contentLengthMin,
            @RequestParam(required = false) Integer contentLengthMax,
            @RequestParam(required = false) String userId,
            @RequestParam(required = false) String userIds
    ) {
        long startTime = System.currentTimeMillis();
        Map<String, Object> response = new HashMap<>();
        
        try {
            // 解析用户ID列表
            Set<String> userIdSet = new HashSet<>();
            if (userId != null && !userId.isEmpty()) {
                userIdSet.add(userId);
            }
            if (userIds != null && !userIds.isEmpty()) {
                userIdSet.addAll(Arrays.asList(userIds.split(",")));
            }
            
            // 设置默认日期范围（最近3天）
            if (endDate == null) endDate = LocalDate.parse("2019-07-12");
            if (startDate == null) startDate = endDate.minusDays(2);
            
            // 验证日期范围
            LocalDate DATA_MIN_DATE = LocalDate.parse("2019-06-13");
            LocalDate DATA_MAX_DATE = LocalDate.parse("2019-07-12");
            
            if (startDate.isBefore(DATA_MIN_DATE)) startDate = DATA_MIN_DATE;
            if (endDate.isAfter(DATA_MAX_DATE)) endDate = DATA_MAX_DATE;
            
            // 根据查询条件选择策略
            boolean hasFilters = topic != null || titleLengthMin != null || titleLengthMax != null || 
                               contentLengthMin != null || contentLengthMax != null || !userIdSet.isEmpty();
            
            Map<String, Object> data = new HashMap<>();
            
            if (!hasFilters) {
                // 策略1：纯Redis查询（最快）
                data.put("totalClicks", calculateTotalClicksFromRedis(startDate, endDate));
                data.put("newsStats", getTopNewsFromRedis(startDate, endDate, 20));
                data.put("userStats", getTopUsersFromRedis(startDate, endDate, 20));
            } else {
                // 策略2：混合查询
                data.put("totalClicks", calculateTotalClicksWithFilters(startDate, endDate, topic, 
                    titleLengthMin, titleLengthMax, contentLengthMin, contentLengthMax, userIdSet));
                data.put("newsStats", getNewsStatsWithFilters(startDate, endDate, topic, 
                    titleLengthMin, titleLengthMax, contentLengthMin, contentLengthMax, userIdSet));
                data.put("userStats", getUserStatsWithFilters(startDate, endDate, topic, 
                    titleLengthMin, titleLengthMax, contentLengthMin, contentLengthMax, userIdSet));
            }
            
            response.put("code", 200);
            response.put("message", "success");
            response.put("timestamp", Instant.now().toEpochMilli());
            response.put("elapsed", System.currentTimeMillis() - startTime);
            response.put("data", data);
            
        } catch (Exception e) {
            response.put("code", 500);
            response.put("message", "Error: " + e.getMessage());
            response.put("timestamp", Instant.now().toEpochMilli());
            response.put("elapsed", System.currentTimeMillis() - startTime);
            e.printStackTrace();
        }
        
        return response;
    }
    
    // 策略1：纯Redis查询 - 计算总点击量
    private long calculateTotalClicksFromRedis(LocalDate startDate, LocalDate endDate) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        long total = 0;
        
        LocalDate current = startDate;
        while (!current.isAfter(endDate)) {
            String dayKey = "news_hot_rank_daily:" + current.format(formatter);
            
            // 只统计总数，不获取详细数据
            Set<ZSetOperations.TypedTuple<String>> topNews = 
                stringRedisTemplate.opsForZSet().reverseRangeWithScores(dayKey, 0, -1);
            
            if (topNews != null) {
                for (ZSetOperations.TypedTuple<String> tuple : topNews) {
                    if (tuple.getScore() != null) {
                        total += tuple.getScore().longValue();
                    }
                }
            }
            current = current.plusDays(1);
        }
        
        return total;
    }
    
    // 策略1：纯Redis查询 - 获取热门新闻
    private List<Map<String, Object>> getTopNewsFromRedis(LocalDate startDate, LocalDate endDate, int limit) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        Map<String, Long> newsClickMap = new HashMap<>();
        
        LocalDate current = startDate;
        while (!current.isAfter(endDate)) {
            String dayKey = "news_hot_rank_daily:" + current.format(formatter);
            
            // 获取每天的Top新闻
            Set<ZSetOperations.TypedTuple<String>> topNews = 
                stringRedisTemplate.opsForZSet().reverseRangeWithScores(dayKey, 0, 99);
            
            if (topNews != null) {
                for (ZSetOperations.TypedTuple<String> tuple : topNews) {
                    if (tuple.getValue() != null && tuple.getScore() != null) {
                        newsClickMap.merge(tuple.getValue(), tuple.getScore().longValue(), Long::sum);
                    }
                }
            }
            current = current.plusDays(1);
        }
        
        // 排序并返回Top N
        return newsClickMap.entrySet().stream()
            .sorted((a, b) -> Long.compare(b.getValue(), a.getValue()))
            .limit(limit)
            .map(entry -> {
                Map<String, Object> stat = new HashMap<>();
                stat.put("newsId", entry.getKey());
                stat.put("clickCount", entry.getValue());
                return stat;
            })
            .collect(Collectors.toList());
    }
    
    // 策略1：纯Redis查询 - 获取活跃用户
    private List<Map<String, Object>> getTopUsersFromRedis(LocalDate startDate, LocalDate endDate, int limit) {
        // 方案1：从user_zset获取用户，然后统计他们的点击数
        Set<String> topUsers = stringRedisTemplate.opsForZSet().range("user_zset", 0, 99);
        
        if (topUsers == null || topUsers.isEmpty()) {
            return new ArrayList<>();
        }
        
        Map<String, Long> userClickMap = new HashMap<>();
        
        for (String userId : topUsers) {
            // 获取用户已读新闻数量作为点击数
            Long clickCount = stringRedisTemplate.opsForSet().size("user_seen_news:" + userId);
            if (clickCount != null && clickCount > 0) {
                userClickMap.put(userId, clickCount);
            }
        }
        
        return userClickMap.entrySet().stream()
            .sorted((a, b) -> Long.compare(b.getValue(), a.getValue()))
            .limit(limit)
            .map(entry -> {
                Map<String, Object> stat = new HashMap<>();
                stat.put("userId", entry.getKey());
                stat.put("clickCount", entry.getValue());
                return stat;
            })
            .collect(Collectors.toList());
    }
    
    // 策略2：带过滤条件的查询 - 计算总点击量
    private long calculateTotalClicksWithFilters(LocalDate startDate, LocalDate endDate, 
                                                 String topic, Integer titleLengthMin, Integer titleLengthMax,
                                                 Integer contentLengthMin, Integer contentLengthMax, 
                                                 Set<String> userIds) {
        // 构建SQL查询
        StringBuilder sql = new StringBuilder();
        List<Object> params = new ArrayList<>();
        
        sql.append("SELECT COUNT(*) FROM user_clicklog uc ");
        sql.append("JOIN static_news n ON uc.clicknews_id = n.news_id ");
        sql.append("WHERE uc.pos_or_neg = 1 ");
        sql.append("AND uc.click_time BETWEEN ? AND ? ");
        params.add(startDate.atStartOfDay());
        params.add(endDate.atTime(23, 59, 59));
        
        if (topic != null) {
            sql.append("AND n.topic = ? ");
            params.add(topic);
        }
        if (titleLengthMin != null) {
            sql.append("AND n.headline_length >= ? ");
            params.add(titleLengthMin);
        }
        if (titleLengthMax != null) {
            sql.append("AND n.headline_length <= ? ");
            params.add(titleLengthMax);
        }
        if (contentLengthMin != null) {
            sql.append("AND n.body_length >= ? ");
            params.add(contentLengthMin);
        }
        if (contentLengthMax != null) {
            sql.append("AND n.body_length <= ? ");
            params.add(contentLengthMax);
        }
        if (!userIds.isEmpty()) {
            sql.append("AND uc.user_id IN (");
            sql.append(String.join(",", Collections.nCopies(userIds.size(), "?")));
            sql.append(") ");
            params.addAll(userIds);
        }
        
        Long count = jdbcTemplate.queryForObject(sql.toString(), Long.class, params.toArray());
        return count != null ? count : 0L;
    }
    
    // 策略2：带过滤条件的查询 - 获取新闻统计
    private List<Map<String, Object>> getNewsStatsWithFilters(LocalDate startDate, LocalDate endDate,
                                                              String topic, Integer titleLengthMin, Integer titleLengthMax,
                                                              Integer contentLengthMin, Integer contentLengthMax,
                                                              Set<String> userIds) {
        StringBuilder sql = new StringBuilder();
        List<Object> params = new ArrayList<>();
        
        sql.append("SELECT uc.clicknews_id as newsId, COUNT(*) as clickCount ");
        sql.append("FROM user_clicklog uc ");
        sql.append("JOIN static_news n ON uc.clicknews_id = n.news_id ");
        sql.append("WHERE uc.pos_or_neg = 1 ");
        sql.append("AND uc.click_time BETWEEN ? AND ? ");
        params.add(startDate.atStartOfDay());
        params.add(endDate.atTime(23, 59, 59));
        
        if (topic != null) {
            sql.append("AND n.topic = ? ");
            params.add(topic);
        }
        if (titleLengthMin != null) {
            sql.append("AND n.headline_length >= ? ");
            params.add(titleLengthMin);
        }
        if (titleLengthMax != null) {
            sql.append("AND n.headline_length <= ? ");
            params.add(titleLengthMax);
        }
        if (contentLengthMin != null) {
            sql.append("AND n.body_length >= ? ");
            params.add(contentLengthMin);
        }
        if (contentLengthMax != null) {
            sql.append("AND n.body_length <= ? ");
            params.add(contentLengthMax);
        }
        if (!userIds.isEmpty()) {
            sql.append("AND uc.user_id IN (");
            sql.append(String.join(",", Collections.nCopies(userIds.size(), "?")));
            sql.append(") ");
            params.addAll(userIds);
        }
        
        sql.append("GROUP BY uc.clicknews_id ");
        sql.append("ORDER BY clickCount DESC ");
        sql.append("LIMIT 20");
        
        return jdbcTemplate.query(sql.toString(), params.toArray(), (rs, rowNum) -> {
            Map<String, Object> stat = new HashMap<>();
            stat.put("newsId", rs.getString("newsId"));
            stat.put("clickCount", rs.getLong("clickCount"));
            return stat;
        });
    }
    
    // 策略2：带过滤条件的查询 - 获取用户统计
    private List<Map<String, Object>> getUserStatsWithFilters(LocalDate startDate, LocalDate endDate,
                                                              String topic, Integer titleLengthMin, Integer titleLengthMax,
                                                              Integer contentLengthMin, Integer contentLengthMax,
                                                              Set<String> userIds) {
        StringBuilder sql = new StringBuilder();
        List<Object> params = new ArrayList<>();
        
        sql.append("SELECT uc.user_id as userId, COUNT(*) as clickCount ");
        sql.append("FROM user_clicklog uc ");
        sql.append("JOIN static_news n ON uc.clicknews_id = n.news_id ");
        sql.append("WHERE uc.pos_or_neg = 1 ");
        sql.append("AND uc.click_time BETWEEN ? AND ? ");
        params.add(startDate.atStartOfDay());
        params.add(endDate.atTime(23, 59, 59));
        
        if (topic != null) {
            sql.append("AND n.topic = ? ");
            params.add(topic);
        }
        if (titleLengthMin != null) {
            sql.append("AND n.headline_length >= ? ");
            params.add(titleLengthMin);
        }
        if (titleLengthMax != null) {
            sql.append("AND n.headline_length <= ? ");
            params.add(titleLengthMax);
        }
        if (contentLengthMin != null) {
            sql.append("AND n.body_length >= ? ");
            params.add(contentLengthMin);
        }
        if (contentLengthMax != null) {
            sql.append("AND n.body_length <= ? ");
            params.add(contentLengthMax);
        }
        if (!userIds.isEmpty()) {
            sql.append("AND uc.user_id IN (");
            sql.append(String.join(",", Collections.nCopies(userIds.size(), "?")));
            sql.append(") ");
            params.addAll(userIds);
        }
        
        sql.append("GROUP BY uc.user_id ");
        sql.append("ORDER BY clickCount DESC ");
        sql.append("LIMIT 20");
        
        return jdbcTemplate.query(sql.toString(), params.toArray(), (rs, rowNum) -> {
            Map<String, Object> stat = new HashMap<>();
            stat.put("userId", rs.getString("userId"));
            stat.put("clickCount", rs.getLong("clickCount"));
            return stat;
        });
    }
}