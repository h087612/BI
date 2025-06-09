package org.example.bi.Repository;

import org.example.bi.Entity.News;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
public interface NewsRepository extends JpaRepository<News, String> {

    Page<News> findByCategoryContainingIgnoreCaseAndTopicContainingIgnoreCaseAndHeadlineContainingIgnoreCaseOrTitleEntityContainingIgnoreCase(
            String category, String topic, String headlineKeyword, String entityKeyword, Pageable pageable
    );
}