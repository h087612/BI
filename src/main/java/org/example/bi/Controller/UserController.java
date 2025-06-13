package org.example.bi.Controller;


import jakarta.annotation.Resource;
import org.example.bi.DTO.BrowseHistoryItem;
import org.example.bi.DTO.PopularityResult;
import org.example.bi.Entity.News;
import org.example.bi.Repository.NewsRepository;
import org.example.bi.Repository.UserClickRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.*;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/users")
public class UserController {

    @Autowired
    private UserClickRepository userClickRepository;

    @GetMapping("/{userId}/browse-history")
    public ResponseEntity<?> getUserBrowseHistory(
            @PathVariable String userId,
            @RequestParam(value = "page", defaultValue = "1") int page,
            @RequestParam(value = "pageSize", defaultValue = "20") int pageSize
    ) {
        int offset = (page - 1) * pageSize;

        List<Object[]> rawData = userClickRepository.findBrowseHistory(userId, pageSize, offset);
        List<BrowseHistoryItem> items = rawData.stream().map(row -> {
            BrowseHistoryItem item = new BrowseHistoryItem();
            item.setTimestamp(((Timestamp) row[0]).getTime());
            item.setNewsId((String) row[1]);
            item.setCategory((String) row[2]);
            item.setHeadline((String) row[3]);
            return item;
        }).collect(Collectors.toList());

        long total = userClickRepository.countByUserId(userId);

        Map<String, Object> data = new HashMap<>();
        data.put("total", total);
        data.put("page", page);
        data.put("pageSize", pageSize);
        data.put("items", items);

        Map<String, Object> response = new HashMap<>();
        response.put("code", 200);
        response.put("message", "success");
        response.put("timestamp", System.currentTimeMillis());
        response.put("elapsed", 30);
        response.put("data", data);

        return ResponseEntity.ok(response);
    }
}
