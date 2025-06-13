package org.example.bi.Repository;

import org.example.bi.Entity.News;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface NewsRepository extends JpaRepository<News, String> {

    @Query("SELECT n FROM News n WHERE " +
            "LOWER(n.category) LIKE LOWER(CONCAT('%', :category, '%')) AND " +
            "LOWER(n.topic) LIKE LOWER(CONCAT('%', :topic, '%')) AND (" +
            "LOWER(n.headline) LIKE LOWER(CONCAT('%', :searchText, '%')) OR " +
            "LOWER(n.titleEntity) LIKE LOWER(CONCAT('%', :searchText, '%')))")
    Page<News> searchNews(
            @Param("category") String category,
            @Param("topic") String topic,
            @Param("searchText") String searchText,
            Pageable pageable
    );

    @Query("SELECT DISTINCT s.category FROM News s")
    List<String> findAllDistinctCategories();


}