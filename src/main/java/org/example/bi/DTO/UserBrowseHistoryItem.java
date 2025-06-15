package org.example.bi.DTO;

import com.fasterxml.jackson.annotation.JsonProperty;

public class UserBrowseHistoryItem {
    @JsonProperty("timestamp")
    private Long timestamp;
    
    @JsonProperty("category")
    private String category;
    
    @JsonProperty("score")
    private Double score;
    
    public UserBrowseHistoryItem() {
    }
    
    public UserBrowseHistoryItem(Long timestamp, String category, Double score) {
        this.timestamp = timestamp;
        this.category = category;
        this.score = score;
    }
    
    public Long getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
    
    public String getCategory() {
        return category;
    }
    
    public void setCategory(String category) {
        this.category = category;
    }
    
    public Double getScore() {
        return score;
    }
    
    public void setScore(Double score) {
        this.score = score;
    }
}