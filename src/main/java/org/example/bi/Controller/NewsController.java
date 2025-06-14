package org.example.bi.Controller;


import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import org.example.bi.DTO.PopularityResult;
import org.example.bi.Entity.News;
import org.example.bi.Repository.NewsRepository;
import org.example.bi.Repository.UserClickRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.domain.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/news")
public class NewsController {

    @Resource
    private NewsRepository newsRepository;

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    private DefaultRedisScript<List> recommendScript;

    @Autowired
    private UserClickRepository userClickRepository;

    @PostConstruct
    public void initLua() {
        recommendScript = new DefaultRedisScript<>();
        recommendScript.setLocation(new ClassPathResource("recommend_v2.lua")); // resources/recommend.lua
        recommendScript.setResultType(List.class);                          // 返回 List<String>
    }
    // 获取新闻列表
    @GetMapping
    public Map<String, Object> getNewsList(
            @RequestParam(required = false, defaultValue = "") String category,
            @RequestParam(required = false, defaultValue = "") String topic,
            @RequestParam(required = false, defaultValue = "") String searchText,
            @RequestParam(required = false, defaultValue = "1") int page,
            @RequestParam(required = false, defaultValue = "20") int pageSize,
            @RequestParam(required = false, defaultValue = "desc") String sortOrder
    ) {
        long start = System.currentTimeMillis();

        Sort sort;
        Pageable pageable;
        
        // 智能选择查询策略
        Page<News> newsPage;
        if (shouldUseFulltext(searchText)) {
            // 对于原生查询，需要使用数据库列名
            sort = Sort.by("publish_time");
            sort = sortOrder.equalsIgnoreCase("asc") ? sort.ascending() : sort.descending();
            pageable = PageRequest.of(page - 1, Math.min(pageSize, 100), sort);
            try {
                newsPage = newsRepository.searchNewsWithFulltext(category, topic, searchText, pageable);
            } catch (Exception e) {
                // 如果全文索引不存在，回退到LIKE查询
                System.out.println("全文索引查询失败，回退到LIKE查询: " + e.getMessage());
                sort = Sort.by("publishTime");
                sort = sortOrder.equalsIgnoreCase("asc") ? sort.ascending() : sort.descending();
                pageable = PageRequest.of(page - 1, Math.min(pageSize, 100), sort);
                newsPage = newsRepository.searchNews(category, topic, searchText, pageable);
            }
        } else {
            // 对于JPQL查询，使用实体属性名
            sort = Sort.by("publishTime");
            sort = sortOrder.equalsIgnoreCase("asc") ? sort.ascending() : sort.descending();
            pageable = PageRequest.of(page - 1, Math.min(pageSize, 100), sort);
            newsPage = newsRepository.searchNews(category, topic, searchText, pageable);
        }

        Map<String, Object> response = new HashMap<>();
        response.put("code", 200);
        response.put("message", "success");
        response.put("timestamp", Instant.now().toEpochMilli());
        response.put("elapsed", System.currentTimeMillis() - start);

        Map<String, Object> data = new HashMap<>();
        data.put("total", newsPage.getTotalElements());
        data.put("page", page);
        data.put("pageSize", newsPage.getSize());
        data.put("items", newsPage.map(news -> Map.of(
                "id", news.getId(),
                "category", news.getCategory(),
                "topic", news.getTopic(),
                "headline", news.getHeadline(),
                "publishDate", news.getPublishTime()
        )).getContent());

        response.put("data", data);
        return response;
    }

    /**
     * 判断是否应该使用全文索引
     * @param searchText 搜索文本
     * @return true 使用全文索引, false 使用 LIKE 查询
     */
    private boolean shouldUseFulltext(String searchText) {
        if (searchText == null || searchText.trim().isEmpty()) {
            return false;
        }
        
        String trimmed = searchText.trim();
        
        // 1. 长度小于3个字符，使用LIKE（MySQL全文索引默认最小词长为3）
        if (trimmed.length() < 3) {
            return false;
        }
        
        // 2. 包含特殊字符或通配符，使用LIKE
        if (trimmed.matches(".*[%_*?\\[\\](){}|\\\\].*")) {
            return false;
        }
        
        // 3. 纯数字或包含大量标点符号，使用LIKE
        if (trimmed.matches("^\\d+$") || trimmed.matches(".*[!@#$&+=:;\"'<>,.]+.*")) {
            return false;
        }
        
        // 4. 单个词且长度小于4，使用LIKE（避免全文索引的停用词问题）
        if (!trimmed.contains(" ") && trimmed.length() < 4) {
            return false;
        }
        
        // 5. 其他情况使用全文索引（自然语言查询）
        return true;
    }

    // 获取单条新闻详情
    @GetMapping("/{newsId}")
    public Map<String, Object> getNewsDetail(@PathVariable String newsId) {
        long start = System.currentTimeMillis();
        News news = newsRepository.findById(newsId).orElse(null);

        Map<String, Object> response = new HashMap<>();
        response.put("code", news != null ? 200 : 404);
        response.put("message", news != null ? "success" : "not found");
        response.put("timestamp", Instant.now().toEpochMilli());
        response.put("elapsed", System.currentTimeMillis() - start);

        if (news != null) {
            response.put("data", Map.of(
                    "id", news.getId(),
                    "category", news.getCategory(),
                    "topic", news.getTopic(),
                    "headline", news.getHeadline(),
                    "body", news.getBody(),
                    "publishDate", news.getPublishTime()
            ));
        }

        return response;
    }

    @GetMapping("/{newsId}/popularity")
    public ResponseEntity<?> getNewsPopularity(
            @PathVariable String newsId,
            @RequestParam("startDate") @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDate startDate,
            @RequestParam("endDate") @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDate endDate,
            @RequestParam(value = "interval", defaultValue = "day") String interval
    ) {
        LocalDateTime start = startDate.atStartOfDay();
        LocalDateTime end = endDate.atTime(23, 59, 59);

        List<PopularityResult> result;

        if ("hour".equalsIgnoreCase(interval)) {
            result = userClickRepository.countByHour(newsId, start, end);
        } else {
            result = userClickRepository.countByDay(newsId, start, end);
        }

        Map<String, Object> response = new HashMap<>();
        response.put("code", 200);
        response.put("message", "success");
        response.put("timestamp", System.currentTimeMillis());
        response.put("elapsed", 20); // 可换成实际耗时
        response.put("data", result);

        return ResponseEntity.ok(response);
    }

    @GetMapping("/categories/popularity")
    public ResponseEntity<?> getCategoryPopularity(
            @RequestParam(required = false) String categories,
            @RequestParam("startDate") @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDate startDate,
            @RequestParam("endDate") @DateTimeFormat(pattern = "yyyy-MM-dd") LocalDate endDate,
            @RequestParam(value = "interval", defaultValue = "day") String interval
    ) {
        List<String> categoryList;
        if (categories == null || categories.isBlank()) {
            categoryList = newsRepository.findAllDistinctCategories(); // 自定义方法
        } else {
            categoryList = Arrays.stream(categories.split(","))
                    .map(String::trim)
                    .collect(Collectors.toList());
        }

        Map<String, List<PopularityResult>> data = new HashMap<>();

        for (String category : categoryList) {
            List<PopularityResult> results;
            if ("hour".equalsIgnoreCase(interval)) {
                results = userClickRepository.countClicksByHourAndCategory(category, startDate.atStartOfDay(), endDate.atTime(23, 59, 59));
            } else {
                results = userClickRepository.countClicksByDayAndCategory(category, startDate.atStartOfDay(), endDate.atTime(23, 59, 59));
            }
            data.put(category, results);
        }

        Map<String, Object> response = new HashMap<>();
        response.put("code", 200);
        response.put("message", "success");
        response.put("timestamp", System.currentTimeMillis());
        response.put("elapsed", 25);
        response.put("data", data);

        return ResponseEntity.ok(response);
    }

    @GetMapping("/users/{userId}/recommendations")
    public Map<String, Object> recommendForUser(
            @PathVariable String userId,
            @RequestParam(name = "topN", defaultValue = "20") int topN,
            @RequestParam String timestamp // <-
    ) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime parsedTime;
        try {
            parsedTime = LocalDateTime.parse(timestamp, formatter);
        } catch (DateTimeParseException e) {
            // 格式不对，返回400
            Map<String, Object> resp = new HashMap<>();
            resp.put("code", 400);
            resp.put("message", "Invalid timestamp format (expect: yyyy-MM-dd HH:mm:ss)");
            return resp;
        }

        long t0 = System.currentTimeMillis();
        Map<String, Object> resp = new HashMap<>();

        try {
            /* ---------- 1. 取用户兴趣 Top K 类别 ---------- */
            final int K = 5;                                // 取前 K 个类别
            String interestKey = "user_cate_score:" + userId;
            // HGETALL → Map<String,String>
            Map<Object, Object> raw = stringRedisTemplate.opsForHash().entries(interestKey);

            if (raw.isEmpty()) {                // 用户没有兴趣画像，直接返回空
                resp.put("code", 200);
                resp.put("message", "success");
                resp.put("timestamp", timestamp);
                resp.put("elapsed", System.currentTimeMillis() - t0);
                resp.put("data", Collections.emptyList());
                return resp;
            }

            // 转成 (cate,score) 并排序
            List<Map.Entry<String, Double>> sorted = raw.entrySet()
                    .stream()
                    .map(e -> Map.entry((String) e.getKey(), Double.valueOf(e.getValue().toString())))
                    .sorted((a, b) -> Double.compare(b.getValue(), a.getValue()))
                    .limit(K)
                    .collect(Collectors.toList());

            /* ---------- 2. 组装 KEYS / ARGV ---------- */
            DateTimeFormatter dayFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
            String dayStr = parsedTime.format(dayFormatter);

            List<String> keys = new ArrayList<>();
            List<String> argv = new ArrayList<>();

            // 2.1 类别热榜 ZSet + 权重
            for (Map.Entry<String, Double> e : sorted) {
                String cate = e.getKey();
                double weight = e.getValue();
                keys.add("news_hot_rank_daily:" + cate + ":" + dayStr);
                argv.add(String.valueOf(weight));
            }

            // 2.2 已读 / 不喜欢 Set
            keys.add("user_seen_news:" + userId);
            keys.add("user_dislike_news:" + userId);

            // 2.3 最后一个 ARGV = topN
            argv.add(String.valueOf(topN));

            /* ---------- 3. 执行 Lua ----------
             * recommendScript_v2 已在 @PostConstruct 中指向 recommend_v2.lua
             */
            @SuppressWarnings("unchecked")
            List<String> newsIds = (List<String>) stringRedisTemplate
                    .execute(recommendScript, keys, argv.toArray());

            if (newsIds == null) newsIds = Collections.emptyList();

            /* ---------- 4. 查 headline，保证返回顺序一致 ---------- */
            List<News> newsList = newsIds.isEmpty()
                    ? Collections.emptyList()
                    : newsRepository.findAllById(newsIds);

            Map<String, String> hMap = newsList.stream()
                    .collect(Collectors.toMap(News::getId, News::getHeadline));

            List<Map<String, Object>> data = newsIds.stream()
                    .map(id -> {
                        Map<String, Object> m = new HashMap<>();
                        m.put("newsId", id);
                        m.put("headline", hMap.getOrDefault(id, ""));
                        return m;
                    })
                    .collect(Collectors.toList());


            /* ---------- 5. 输出 ---------- */
            resp.put("code", 200);
            resp.put("message", "success");
            resp.put("timestamp", timestamp);
            resp.put("elapsed", System.currentTimeMillis() - t0);
            resp.put("data", data);
            return resp;

        } catch (Exception ex) {
            resp.put("code", 500);
            resp.put("message", "Error: " + ex.getClass().getName() + " - " + (ex.getMessage() != null ? ex.getMessage() : "No message"));
            resp.put("stack", Arrays.stream(ex.getStackTrace()).map(StackTraceElement::toString).limit(5).collect(Collectors.toList()));
            resp.put("timestamp", timestamp);
            resp.put("elapsed", System.currentTimeMillis() - t0);
            resp.put("data", Collections.emptyList());
            ex.printStackTrace();
            return resp;
        }
    }

}
