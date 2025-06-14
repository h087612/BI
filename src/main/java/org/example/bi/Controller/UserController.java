package org.example.bi.Controller;

import jakarta.annotation.Resource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/users")
public class UserController {
    
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    
    private static final String USER_ZSET_KEY = "user_zset";
    private static final String USER_SET_KEY = "user_set"; // 兼容旧数据
    
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
}