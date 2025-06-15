package org.example.bi.DTO;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class UserInterestResponse {
    @JsonProperty("total")
    private Integer total;
    
    @JsonProperty("page")
    private Integer page;
    
    @JsonProperty("pageSize")
    private Integer pageSize;
    
    @JsonProperty("items")
    private List<UserInterestItem> items;
    
    public UserInterestResponse() {
    }
    
    public UserInterestResponse(Integer total, Integer page, Integer pageSize, List<UserInterestItem> items) {
        this.total = total;
        this.page = page;
        this.pageSize = pageSize;
        this.items = items;
    }
    
    public Integer getTotal() {
        return total;
    }
    
    public void setTotal(Integer total) {
        this.total = total;
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
    
    public List<UserInterestItem> getItems() {
        return items;
    }
    
    public void setItems(List<UserInterestItem> items) {
        this.items = items;
    }
}