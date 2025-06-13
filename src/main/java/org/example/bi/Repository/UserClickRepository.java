package org.example.bi.Repository;

import org.example.bi.DTO.ClickStatDto;
import org.example.bi.DTO.PopularityResult;
import org.example.bi.Entity.UserClick;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.util.List;

public interface UserClickRepository extends JpaRepository<UserClick,Long> {

    @Query(value = "SELECT DATE_FORMAT(uc.click_time, '%Y-%m-%d') AS date, COUNT(*) AS count " +
            "FROM user_clicklog uc " +
            "WHERE uc.clicknews_id = :newsId AND uc.click_time BETWEEN :start AND :end AND uc.pos_or_neg = '1' " +
            "GROUP BY DATE_FORMAT(uc.click_time, '%Y-%m-%d') " +
            "ORDER BY date", nativeQuery = true)
    List<PopularityResult> countByDay(@Param("newsId") String newsId,
                                      @Param("start") LocalDateTime start,
                                      @Param("end") LocalDateTime end);

    @Query(value = "SELECT DATE_FORMAT(uc.click_time, '%Y-%m-%d %H') AS date, COUNT(*) AS count " +
            "FROM user_clicklog uc " +
            "WHERE uc.clicknews_id = :newsId AND uc.click_time BETWEEN :start AND :end AND uc.pos_or_neg = '1' " +
            "GROUP BY DATE_FORMAT(uc.click_time, '%Y-%m-%d %H') " +
            "ORDER BY date", nativeQuery = true)
    List<PopularityResult> countByHour(@Param("newsId") String newsId,
                                       @Param("start") LocalDateTime start,
                                       @Param("end") LocalDateTime end);


    @Query(value = """
    SELECT DATE_FORMAT(uc.click_time, '%Y-%m-%d') AS date, COUNT(*) AS count
    FROM user_clicklog uc
    JOIN static_news sn ON uc.clicknews_id = sn.news_id
    WHERE sn.category = :category
      AND uc.pos_or_neg = 1
      AND uc.click_time BETWEEN :start AND :end
    GROUP BY DATE_FORMAT(uc.click_time, '%Y-%m-%d')
    ORDER BY date
""", nativeQuery = true)
    List<PopularityResult> countClicksByDayAndCategory(@Param("category") String category,
                                       @Param("start") LocalDateTime start,
                                       @Param("end") LocalDateTime end);

    @Query(value = """
    SELECT DATE_FORMAT(uc.click_time, '%Y-%m-%d %H') AS date, COUNT(*) AS count
    FROM user_clicklog uc
    JOIN static_news sn ON uc.clicknews_id = sn.news_id
    WHERE sn.category = :category
      AND uc.pos_or_neg = 1
      AND uc.click_time BETWEEN :start AND :end
    GROUP BY DATE_FORMAT(uc.click_time, '%Y-%m-%d %H')
    ORDER BY date
""", nativeQuery = true)
    List<PopularityResult> countClicksByHourAndCategory(@Param("category") String category,
                                        @Param("start") LocalDateTime start,
                                        @Param("end") LocalDateTime end);


    @Query(value = """
    SELECT 
        uc.click_time AS timestamp,
        uc.clicknews_id AS newsId,
        sn.category AS category,
        sn.headline AS headline
    FROM user_clicklog uc
    JOIN static_news sn ON uc.clicknews_id = sn.news_id
    WHERE uc.user_id = :userId
    ORDER BY uc.click_time DESC
    LIMIT :limit OFFSET :offset
""", nativeQuery = true)
    List<Object[]> findBrowseHistory(@Param("userId") String userId,
                                     @Param("limit") int limit,
                                     @Param("offset") int offset);

    @Query(value = "SELECT COUNT(*) FROM user_clicklog WHERE user_id = :userId", nativeQuery = true)
    Long countByUserId(@Param("userId") String userId);


    @Query("""
        SELECT n.id AS newsId, n.category AS category, c.userId AS userId, COUNT(c.id) AS clickCount
        FROM UserClick  c
        JOIN News n ON c.clickNewsId = n.id
        WHERE (:startDate IS NULL OR c.clickTime >= :startDate)
          AND (:endDate IS NULL OR c.clickTime <= :endDate)
          AND (:category IS NULL OR n.category = :category)
          AND (:titleLengthMin IS NULL OR n.headlineLength >= :titleLengthMin)
          AND (:titleLengthMax IS NULL OR n.headlineLength <= :titleLengthMax)
          AND (:contentLengthMin IS NULL OR n.bodyLength >= :contentLengthMin)
          AND (:contentLengthMax IS NULL OR n.bodyLength <= :contentLengthMax)
          AND (:userIds IS NULL OR c.userId IN :userIds)
        GROUP BY n.id, n.category, c.userId
    """)
    List<ClickStatDto> getStatistics(
            @Param("startDate") LocalDateTime startDate,
            @Param("endDate") LocalDateTime endDate,
            @Param("category") String category,
            @Param("titleLengthMin") Integer titleLengthMin,
            @Param("titleLengthMax") Integer titleLengthMax,
            @Param("contentLengthMin") Integer contentLengthMin,
            @Param("contentLengthMax") Integer contentLengthMax,
            @Param("userIds") List<String> userIds
    );
}
