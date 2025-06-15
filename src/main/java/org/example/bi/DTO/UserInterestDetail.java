package org.example.bi.DTO;

import com.fasterxml.jackson.annotation.JsonProperty;

public class UserInterestDetail {
    @JsonProperty("category")
    private String category;
    
    @JsonProperty("score")
    private Float score;
    
    public UserInterestDetail() {
    }
    
    public UserInterestDetail(String category, Float score) {
        this.category = category;
        this.score = score;
    }
    
    public String getCategory() {
        return category;
    }
    
    public void setCategory(String category) {
        this.category = category;
    }
    
    public Float getScore() {
        return score;
    }
    
    public void setScore(Float score) {
        this.score = score;
    }
}