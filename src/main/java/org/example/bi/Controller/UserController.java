package org.example.bi.Controller;

import jakarta.annotation.Resource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.*;
import org.example.bi.DTO.UserBrowseHistoryResponse;
import org.example.bi.DTO.UserBrowseHistoryItem;
import org.example.bi.DTO.UserInterestResponse;
import org.example.bi.DTO.UserInterestItem;
import org.example.bi.DTO.UserInterestDetail;
import org.springframework.data.redis.core.ZSetOperations;

import java.time.Instant;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/users")
public class UserController {
    
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    
    private static final String USER_ZSET_KEY = "user_zset";
    private static final String USER_SET_KEY = "user_set"; // 兼容旧数据
    private static final LocalDate DATA_MIN_DATE = LocalDate.parse("2019-06-13");
    private static final LocalDate DATA_MAX_DATE = LocalDate.parse("2019-07-12");
    @GetMapping
    public Map<String, Object> getAllUsers(
            @RequestParam(required = false, defaultValue = "1") int page,
            @RequestParam(required = false, defaultValue = "20") int pageSize
    ) {
        long start = System.currentTimeMillis();
        Map<String, Object> response = new HashMap<>();
        
        try {
            // 优先使用 ZSET
            Long totalInZSet = stringRedisTemplate.opsForZSet().zCard(USER_ZSET_KEY);
            
            if (totalInZSet != null && totalInZSet > 0) {
                // 使用 ZSET 的高效分页查询
                long startIndex = (long) (page - 1) * pageSize;
                long endIndex = startIndex + pageSize - 1;
                
                Set<String> userIds = stringRedisTemplate.opsForZSet()
                        .range(USER_ZSET_KEY, startIndex, endIndex);
                
                List<Map<String, String>> items = userIds != null ? 
                    userIds.stream()
                        .map(userId -> Map.of("id", userId))
                        .collect(Collectors.toList()) : 
                    Collections.emptyList();
                
                Map<String, Object> data = new HashMap<>();
                data.put("total", totalInZSet);
                data.put("page", page);
                data.put("pageSize", items.size());
                data.put("items", items);
                
                response.put("code", 200);
                response.put("message", "success");
                response.put("timestamp", Instant.now().toEpochMilli());
                response.put("elapsed", System.currentTimeMillis() - start);
                response.put("data", data);
                
            } else {
                // 降级到 SET 查询（兼容旧数据）
                Set<String> allUserIds = stringRedisTemplate.opsForSet().members(USER_SET_KEY);
                
                if (allUserIds == null || allUserIds.isEmpty()) {
                    // 返回空结果
                    Map<String, Object> data = new HashMap<>();
                    data.put("total", 0);
                    data.put("page", page);
                    data.put("pageSize", pageSize);
                    data.put("items", Collections.emptyList());
                    
                    response.put("code", 200);
                    response.put("message", "success");
                    response.put("timestamp", Instant.now().toEpochMilli());
                    response.put("elapsed", System.currentTimeMillis() - start);
                    response.put("data", data);
                    return response;
                }
                
                // 转换为List并排序
                List<String> sortedUserIds = new ArrayList<>(allUserIds);
                Collections.sort(sortedUserIds);
                
                // 计算分页
                int total = sortedUserIds.size();
                int startIndex = (page - 1) * pageSize;
                int endIndex = Math.min(startIndex + pageSize, total);
                
                List<Map<String, String>> items = Collections.emptyList();
                if (startIndex < total) {
                    items = sortedUserIds.subList(startIndex, endIndex).stream()
                            .map(userId -> Map.of("id", userId))
                            .collect(Collectors.toList());
                }
                
                Map<String, Object> data = new HashMap<>();
                data.put("total", total);
                data.put("page", page);
                data.put("pageSize", items.size());
                data.put("items", items);
                
                response.put("code", 200);
                response.put("message", "success (using SET fallback)");
                response.put("timestamp", Instant.now().toEpochMilli());
                response.put("elapsed", System.currentTimeMillis() - start);
                response.put("data", data);
            }
            
        } catch (Exception e) {
            response.put("code", 500);
            response.put("message", "Error: " + e.getMessage());
            response.put("timestamp", Instant.now().toEpochMilli());
            response.put("elapsed", System.currentTimeMillis() - start);
        }
        
        return response;
    }
    
    /**
     * 获取用户总数（不需要加载所有数据）
     */
    @GetMapping("/count")
    public Map<String, Object> getUserCount() {
        long start = System.currentTimeMillis();
        Map<String, Object> response = new HashMap<>();
        
        try {
            // 优先从 ZSET 获取
            Long count = stringRedisTemplate.opsForZSet().zCard(USER_ZSET_KEY);
            
            // 如果 ZSET 为空，尝试从 SET 获取
            if (count == null || count == 0) {
                count = stringRedisTemplate.opsForSet().size(USER_SET_KEY);
            }
            
            response.put("code", 200);
            response.put("message", "success");
            response.put("timestamp", Instant.now().toEpochMilli());
            response.put("elapsed", System.currentTimeMillis() - start);
            response.put("data", Map.of("count", count != null ? count : 0));
            
        } catch (Exception e) {
            response.put("code", 500);
            response.put("message", "Error: " + e.getMessage());
            response.put("timestamp", Instant.now().toEpochMilli());
            response.put("elapsed", System.currentTimeMillis() - start);
        }
        
        return response;
    }
    
    /**
     * 获取用户浏览历史（兴趣分类）
     */
    @GetMapping("/{userId}/browse-history")
    public Map<String, Object> getUserBrowseHistory(
            @PathVariable String userId,
            @RequestParam(required = false, defaultValue = "1") int page,
            @RequestParam(required = false, defaultValue = "20") int pageSize
    ) {
        long start = System.currentTimeMillis();
        Map<String, Object> response = new HashMap<>();
        
        try {
            // 验证参数
            if (page < 1) page = 1;
            if (pageSize < 1) pageSize = 20;
            if (pageSize > 100) pageSize = 100; // 防止请求过多数据
            
            // 获取用户分类评分数据
            String userCateScoreKey = "user_cate_score:" + userId;
            
            // 先检查键是否存在
            Boolean exists = stringRedisTemplate.hasKey(userCateScoreKey);
            if (exists == null || !exists) {
                // 返回空结果
                UserBrowseHistoryResponse data = new UserBrowseHistoryResponse(0, page, pageSize, Collections.emptyList());
                
                response.put("code", 200);
                response.put("message", "success");
                response.put("timestamp", Instant.now().toEpochMilli());
                response.put("elapsed", System.currentTimeMillis() - start);
                response.put("data", data);
                return response;
            }
            
            // 从Hash中获取所有分类和评分
            Map<Object, Object> categoryScoresMap = stringRedisTemplate.opsForHash().entries(userCateScoreKey);
            
            if (categoryScoresMap.isEmpty()) {
                // 返回空结果
                UserBrowseHistoryResponse data = new UserBrowseHistoryResponse(0, page, pageSize, Collections.emptyList());
                
                response.put("code", 200);
                response.put("message", "success");
                response.put("timestamp", Instant.now().toEpochMilli());
                response.put("elapsed", System.currentTimeMillis() - start);
                response.put("data", data);
                return response;
            }
            
            // 转换为列表并按分数排序
            List<UserBrowseHistoryItem> allItems = new ArrayList<>();
            long currentTimestamp = Instant.now().toEpochMilli();
            
            for (Map.Entry<Object, Object> entry : categoryScoresMap.entrySet()) {
                String category = entry.getKey().toString();
                Double score = Double.parseDouble(entry.getValue().toString());
                
                // 过滤掉负数评分
                if (score > 0) {
                    UserBrowseHistoryItem item = new UserBrowseHistoryItem(
                        currentTimestamp,
                        category,
                        score
                    );
                    allItems.add(item);
                }
            }
            
            // 按分数降序排序
            allItems.sort((a, b) -> Double.compare(b.getScore(), a.getScore()));
            
            // 计算分页
            int total = allItems.size();
            int startIndex = (page - 1) * pageSize;
            int endIndex = Math.min(startIndex + pageSize, total);
            
            List<UserBrowseHistoryItem> pageItems = Collections.emptyList();
            if (startIndex < total) {
                pageItems = allItems.subList(startIndex, endIndex);
            }
            
            UserBrowseHistoryResponse data = new UserBrowseHistoryResponse(
                total, 
                page, 
                pageItems.size(),
                pageItems
            );
            
            response.put("code", 200);
            response.put("message", "success");
            response.put("timestamp", Instant.now().toEpochMilli());
            response.put("elapsed", System.currentTimeMillis() - start);
            response.put("data", data);
            
        } catch (Exception e) {
            e.printStackTrace();
            response.put("code", 500);
            response.put("message", "Error: " + e.getMessage());
            response.put("timestamp", Instant.now().toEpochMilli());
            response.put("elapsed", System.currentTimeMillis() - start);
            response.put("error_details", e.getClass().getName() + ": " + e.getMessage());
        }
        
        return response;
    }
    
    /**
     * 获取用户兴趣变化
     */
    @GetMapping("/{userId}/interest")
    public Map<String, Object> getUserInterest(
            @PathVariable String userId,
            @RequestParam(required = false) String startDate,
            @RequestParam(required = false) String endDate,
            @RequestParam(required = false, defaultValue = "1") int page,
            @RequestParam(required = false, defaultValue = "20") int pageSize
    ) {
        long start = System.currentTimeMillis();
        Map<String, Object> response = new HashMap<>();
        
        try {
            // 验证参数
            if (page < 1) page = 1;
            if (pageSize < 1) pageSize = 20;
            if (pageSize > 100) pageSize = 100;
            
            // 构建日期范围
            List<String> sortedDates = new ArrayList<>();
            
            // 如果没有指定日期范围，尝试获取最近30天的数据
            if (startDate == null && endDate == null) {
                // 获取当前日期和30天前的日期
                java.time.LocalDate now = java.time.LocalDate.now();
                java.time.LocalDate thirtyDaysAgo = now.minusDays(30);
                
                // 生成日期列表
                for (java.time.LocalDate date = now; !date.isBefore(thirtyDaysAgo); date = date.minusDays(1)) {
                    String dateStr = date.format(java.time.format.DateTimeFormatter.BASIC_ISO_DATE);
                    String dateKey = "user_cate_score:" + userId + ":" + dateStr;
                    
                    // 检查这个键是否存在
                    Boolean exists = stringRedisTemplate.hasKey(dateKey);
                    if (exists != null && exists) {
                        sortedDates.add(dateStr);
                    }
                }
            } else {
                // 使用指定的日期范围
                java.time.LocalDate start1 = startDate != null ?
                    java.time.LocalDate.parse(startDate) :
                        DATA_MIN_DATE;
                java.time.LocalDate end = endDate != null ? 
                    java.time.LocalDate.parse(endDate) : 
                    DATA_MAX_DATE;
                
                // 生成日期列表
                for (java.time.LocalDate date = end; !date.isBefore(start1); date = date.minusDays(1)) {
                    String dateStr = date.format(java.time.format.DateTimeFormatter.BASIC_ISO_DATE);
                    String dateKey = "user_cate_score:" + userId + ":" + dateStr;
                    
                    // 检查这个键是否存在
                    Boolean exists = stringRedisTemplate.hasKey(dateKey);
                    if (exists != null && exists) {
                        sortedDates.add(dateStr);
                    }
                }
            }
            
            if (sortedDates.isEmpty()) {
                // 返回空结果
                UserInterestResponse data = new UserInterestResponse(0, page, pageSize, Collections.emptyList());
                
                response.put("code", 200);
                response.put("message", "success");
                response.put("timestamp", Instant.now().toEpochMilli());
                response.put("elapsed", System.currentTimeMillis() - start);
                response.put("data", data);
                return response;
            }
            
            // 排序日期（降序）
            sortedDates.sort(Collections.reverseOrder());
            
            // 计算分页
            int total = sortedDates.size();
            int startIndex = (page - 1) * pageSize;
            int endIndex = Math.min(startIndex + pageSize, total);
            
            List<UserInterestItem> items = new ArrayList<>();
            
            if (startIndex < total) {
                List<String> pageDates = sortedDates.subList(startIndex, endIndex);
                
                for (String dateStr : pageDates) {
                    String dateKey = "user_cate_score:" + userId + ":" + dateStr;
                    Map<Object, Object> categoryScores = stringRedisTemplate.opsForHash().entries(dateKey);
                    
                    if (!categoryScores.isEmpty()) {
                        List<UserInterestDetail> interests = new ArrayList<>();
                        
                        // 转换为详情列表并排序
                        List<Map.Entry<Object, Object>> sortedEntries = new ArrayList<>(categoryScores.entrySet());
                        sortedEntries.sort((a, b) -> {
                            double scoreA = Double.parseDouble(a.getValue().toString());
                            double scoreB = Double.parseDouble(b.getValue().toString());
                            return Double.compare(scoreB, scoreA);
                        });
                        
                        for (Map.Entry<Object, Object> entry : sortedEntries) {
                            UserInterestDetail detail = new UserInterestDetail(
                                entry.getKey().toString(),
                                Float.parseFloat(entry.getValue().toString())
                            );
                            interests.add(detail);
                        }
                        
                        UserInterestItem item = new UserInterestItem(
                            Integer.parseInt(dateStr),
                            interests
                        );
                        items.add(item);
                    }
                }
            }
            
            UserInterestResponse data = new UserInterestResponse(
                total,
                page,
                items.size(),
                items
            );
            
            response.put("code", 200);
            response.put("message", "success");
            response.put("timestamp", Instant.now().toEpochMilli());
            response.put("elapsed", System.currentTimeMillis() - start);
            response.put("data", data);
            
        } catch (Exception e) {
            e.printStackTrace();
            response.put("code", 500);
            response.put("message", "Error: " + e.getMessage());
            response.put("timestamp", Instant.now().toEpochMilli());
            response.put("elapsed", System.currentTimeMillis() - start);
            response.put("error_details", e.getClass().getName() + ": " + e.getMessage());
        }
        
        return response;
    }
}