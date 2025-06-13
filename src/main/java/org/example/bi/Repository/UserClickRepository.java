package org.example.bi.Repository;

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




}
