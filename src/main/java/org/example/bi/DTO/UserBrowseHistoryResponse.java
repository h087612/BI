package org.example.bi.DTO;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class UserBrowseHistoryResponse {
    @JsonProperty("total")
    private Integer total;
    
    @JsonProperty("page")
    private Integer page;
    
    @JsonProperty("pageSize")
    private Integer pageSize;
    
    @JsonProperty("items")
    private List<UserBrowseHistoryItem> items;
    
    public UserBrowseHistoryResponse() {
    }
    
    public UserBrowseHistoryResponse(Integer total, Integer page, Integer pageSize, List<UserBrowseHistoryItem> items) {
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
    
    public List<UserBrowseHistoryItem> getItems() {
        return items;
    }
    
    public void setItems(List<UserBrowseHistoryItem> items) {
        this.items = items;
    }
}