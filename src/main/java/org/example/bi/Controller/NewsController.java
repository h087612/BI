package org.example.bi.Controller;


import jakarta.annotation.Resource;
import org.example.bi.DTO.ClickStatDto;
import org.example.bi.DTO.PopularityResult;
import org.example.bi.Entity.News;
import org.example.bi.Repository.NewsRepository;
import org.example.bi.Repository.UserClickRepository;
import org.example.bi.Service.QueryLogService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.*;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.time.format.DateTimeFormatter;
@RestController
@RequestMapping("/news")
public class NewsController {

    @Resource
    private NewsRepository newsRepository;
    @Autowired
    private UserClickRepository userClickRepository;

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

        Sort sort = Sort.by("publishTime");
        sort = sortOrder.equalsIgnoreCase("asc") ? sort.ascending() : sort.descending();
        Pageable pageable = PageRequest.of(page - 1, Math.min(pageSize, 100), sort);

        Page<News> newsPage = newsRepository
                .searchNews(category, topic, searchText, pageable);

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


    @GetMapping("/statistics")
    public ResponseEntity<?> getStatistics(
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate,
            @RequestParam(required = false) String category,
            @RequestParam(required = false) Integer titleLengthMin,
            @RequestParam(required = false) Integer titleLengthMax,
            @RequestParam(required = false) Integer contentLengthMin,
            @RequestParam(required = false) Integer contentLengthMax,
            @RequestParam(required = false) String userId,
            @RequestParam(required = false) String userIds
    ) {
        long startTime = System.currentTimeMillis();

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDateTime startDateTime = null;
        LocalDateTime endDateTime = null;

        if (startDate != null && !startDate.isBlank()) {
            startDateTime = LocalDate.parse(startDate, formatter).atStartOfDay();
        }
        if (endDate != null && !endDate.isBlank()) {
            endDateTime = LocalDate.parse(endDate, formatter).plusDays(1).atStartOfDay().minusNanos(1);
        }

        // 构建动态条件
        List<String> userIdList = new ArrayList<>();
        if (userId != null && !userId.isBlank()) userIdList.add(userId);
        if (userIds != null && !userIds.isBlank()) {
            userIdList.addAll(Arrays.asList(userIds.split(",")));
        }

        List<ClickStatDto> results = userClickRepository.getStatistics(
                startDateTime, endDateTime, category, titleLengthMin, titleLengthMax,
                contentLengthMin, contentLengthMax, userIdList.isEmpty() ? null : userIdList
        );
        long elapsed = System.currentTimeMillis() - startTime;
        Map<String, Object> response = new HashMap<>();
        response.put("code", 200);
        response.put("message", "success");
        response.put("timestamp", System.currentTimeMillis());
        response.put("elapsed", elapsed);
        response.put("data", results);

        return ResponseEntity.ok(response);
    }
}
