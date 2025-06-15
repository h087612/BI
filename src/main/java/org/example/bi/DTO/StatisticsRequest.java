package org.example.bi.DTO;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDate;

public class StatisticsRequest {
    @JsonProperty("category")
    private String category;
    
    @JsonProperty("contentLengthMax")
    private Long contentLengthMax;
    
    @JsonProperty("contentLengthMin")
    private Long contentLengthMin;
    
    @JsonProperty("dislike")
    private Boolean dislike;
    
    @JsonProperty("endDate")
    @JsonFormat(pattern = "yyyy-MM-dd")
    private LocalDate endDate;
    
    @JsonProperty("like")
    private Boolean like;
    
    @JsonProperty("startDate")
    @JsonFormat(pattern = "yyyy-MM-dd")
    private LocalDate startDate;
    
    @JsonProperty("titleLengthMax")
    private Long titleLengthMax;
    
    @JsonProperty("titleLengthMin")
    private Long titleLengthMin;
    
    @JsonProperty("topic")
    private String topic;
    
    @JsonProperty("userId")
    private String userId;
    
    @JsonProperty("userIds")
    private String userIds;
    
    @JsonProperty("page")
    private Integer page = 1;  // 默认第1页
    
    @JsonProperty("pageSize") 
    private Integer pageSize = 20;  // 默认每页20条

    // Getters and Setters
    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public Long getContentLengthMax() {
        return contentLengthMax;
    }

    public void setContentLengthMax(Long contentLengthMax) {
        this.contentLengthMax = contentLengthMax;
    }

    public Long getContentLengthMin() {
        return contentLengthMin;
    }

    public void setContentLengthMin(Long contentLengthMin) {
        this.contentLengthMin = contentLengthMin;
    }

    public Boolean getDislike() {
        return dislike;
    }

    public void setDislike(Boolean dislike) {
        this.dislike = dislike;
    }

    public LocalDate getEndDate() {
        return endDate;
    }

    public void setEndDate(LocalDate endDate) {
        this.endDate = endDate;
    }

    public Boolean getLike() {
        return like;
    }

    public void setLike(Boolean like) {
        this.like = like;
    }

    public LocalDate getStartDate() {
        return startDate;
    }

    public void setStartDate(LocalDate startDate) {
        this.startDate = startDate;
    }

    public Long getTitleLengthMax() {
        return titleLengthMax;
    }

    public void setTitleLengthMax(Long titleLengthMax) {
        this.titleLengthMax = titleLengthMax;
    }

    public Long getTitleLengthMin() {
        return titleLengthMin;
    }

    public void setTitleLengthMin(Long titleLengthMin) {
        this.titleLengthMin = titleLengthMin;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getUserIds() {
        return userIds;
    }

    public void setUserIds(String userIds) {
        this.userIds = userIds;
    }
    
    public Integer getPage() {
        return page;
    }
    
    public void setPage(Integer page) {
        this.page = page;
    }
    
    public Integer getPageSize() {
        return pageSize;
    }
    
    public void setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
    }
}