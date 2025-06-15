package org.example.bi.DTO;

import com.fasterxml.jackson.annotation.JsonProperty;

public class UserInterestDetail {
    @JsonProperty("category")
    private String category;
    
    @JsonProperty("score")
    private String score;
    
    public UserInterestDetail() {
    }
    
    public UserInterestDetail(String category, String score) {
        this.category = category;
        this.score = score;
    }
    
    public String getCategory() {
        return category;
    }
    
    public void setCategory(String category) {
        this.category = category;
    }
    
    public String getScore() {
        return score;
    }
    
    public void setScore(String score) {
        this.score = score;
    }
}