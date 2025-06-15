package org.example.bi.DTO;

import lombok.Data;

@Data
public class BrowseHistoryItem {
    private Long timestamp;
    private String newsId;
    private String category;
    private String headline;
}
