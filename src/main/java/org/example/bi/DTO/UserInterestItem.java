package org.example.bi.DTO;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class UserInterestItem {
    @JsonProperty("date")
    private Integer date;
    
    @JsonProperty("interests")
    private List<UserInterestDetail> interests;
    
    public UserInterestItem() {
    }
    
    public UserInterestItem(Integer date, List<UserInterestDetail> interests) {
        this.date = date;
        this.interests = interests;
    }
    
    public Integer getDate() {
        return date;
    }
    
    public void setDate(Integer date) {
        this.date = date;
    }
    
    public List<UserInterestDetail> getInterests() {
        return interests;
    }
    
    public void setInterests(List<UserInterestDetail> interests) {
        this.interests = interests;
    }
}