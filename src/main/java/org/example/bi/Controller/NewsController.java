package org.example.bi.Controller;


import jakarta.annotation.Resource;
import org.example.bi.Entity.News;
import org.example.bi.Repository.NewsRepository;
import org.springframework.data.domain.*;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
@RestController
@RequestMapping("/news")
public class NewsController {

    @Resource
    private NewsRepository newsRepository;

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
                .findByCategoryContainingIgnoreCaseAndTopicContainingIgnoreCaseAndHeadlineContainingIgnoreCaseOrTitleEntityContainingIgnoreCase(
                        category, topic, searchText, searchText, pageable
                );

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
    @GetMapping("/news/{newsId}")
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
}
