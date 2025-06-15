package org.example.bi.Entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;

@Data
@Entity
@Table(name = "static_news")
public class News {
    @Id
    @Column(name = "news_id")
    private String id;

    private String category;

    private String topic;

    private String headline;

    @Column(name = "body", columnDefinition = "LONGTEXT")
    private String body;

    @Column(name = "body_length")
    private Integer bodyLength;

    @Column(name = "headline_length")
    private Integer headlineLength;

    @Column(name = "publish_time")
    private LocalDateTime publishTime;

    @Column(name = "title_entity", columnDefinition = "LONGTEXT")
    private String titleEntity;
}
