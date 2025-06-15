package org.example.bi.DTO;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class StatisticsResponse {
    @JsonProperty("code")
    private long code;
    
    @JsonProperty("data")
    private StatisticsData data;
    
    @JsonProperty("elapsed")
    private long elapsed;
    
    @JsonProperty("message")
    private String message;
    
    @JsonProperty("timestamp")
    private long timestamp;

    // Getters and Setters
    public long getCode() {
        return code;
    }

    public void setCode(long code) {
        this.code = code;
    }

    public StatisticsData getData() {
        return data;
    }

    public void setData(StatisticsData data) {
        this.data = data;
    }

    public long getElapsed() {
        return elapsed;
    }

    public void setElapsed(long elapsed) {
        this.elapsed = elapsed;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    // Inner classes
    public static class StatisticsData {
        @JsonProperty("newsStats")
        private List<NewsStat> newsStats;
        
        @JsonProperty("totalClicks")
        private long totalClicks;
        
        @JsonProperty("page")
        private int page;
        
        @JsonProperty("pageSize")
        private int pageSize;
        
        @JsonProperty("totalPages")
        private int totalPages;
        
        @JsonProperty("totalNews")
        private long totalNews;

        // Getters and Setters
        public List<NewsStat> getNewsStats() {
            return newsStats;
        }

        public void setNewsStats(List<NewsStat> newsStats) {
            this.newsStats = newsStats;
        }

        public long getTotalClicks() {
            return totalClicks;
        }

        public void setTotalClicks(long totalClicks) {
            this.totalClicks = totalClicks;
        }
        
        public int getPage() {
            return page;
        }
        
        public void setPage(int page) {
            this.page = page;
        }
        
        public int getPageSize() {
            return pageSize;
        }
        
        public void setPageSize(int pageSize) {
            this.pageSize = pageSize;
        }
        
        public int getTotalPages() {
            return totalPages;
        }
        
        public void setTotalPages(int totalPages) {
            this.totalPages = totalPages;
        }
        
        public long getTotalNews() {
            return totalNews;
        }
        
        public void setTotalNews(long totalNews) {
            this.totalNews = totalNews;
        }
    }

    public static class NewsStat {
        @JsonProperty("clickCount")
        private Long clickCount;
        
        @JsonProperty("newsId")
        private String newsId;

        public NewsStat() {}

        public NewsStat(String newsId, Long clickCount) {
            this.newsId = newsId;
            this.clickCount = clickCount;
        }

        // Getters and Setters
        public Long getClickCount() {
            return clickCount;
        }

        public void setClickCount(Long clickCount) {
            this.clickCount = clickCount;
        }

        public String getNewsId() {
            return newsId;
        }

        public void setNewsId(String newsId) {
            this.newsId = newsId;
        }
    }
}