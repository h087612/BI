package org.example.bi.Repository;

import org.example.bi.Entity.News;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.util.List;

public interface NewsRepository extends JpaRepository<News, String> {

    @Query("SELECT n FROM News n WHERE " +
            "(:category = '' OR n.category = :category) AND " +
            "(:topic = '' OR n.topic = :topic) AND " +
            "(:searchText = '' OR LOWER(n.headline) LIKE LOWER(CONCAT('%', :searchText, '%')))")
    Page<News> searchNews(
            @Param("category") String category,
            @Param("topic") String topic,
            @Param("searchText") String searchText,
            Pageable pageable
    );

    @Query(value = "SELECT * FROM static_news n WHERE " +
            "(:category = '' OR n.category = :category) AND " +
            "(:topic = '' OR n.topic = :topic) AND " +
            "(:searchText = '' OR MATCH(n.headline) AGAINST(:searchText IN NATURAL LANGUAGE MODE))",
            nativeQuery = true)
    Page<News> searchNewsWithFulltext(
            @Param("category") String category,
            @Param("topic") String topic,
            @Param("searchText") String searchText,
            Pageable pageable
    );

    @Query("SELECT DISTINCT s.category FROM News s")
    List<String> findAllDistinctCategories();

    @Query("SELECT n FROM News n WHERE n.id IN :ids")
    List<News> findSimpleInfoByIds(@Param("ids") List<String> ids);

}