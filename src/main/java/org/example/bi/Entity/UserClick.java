package org.example.bi.Entity;


import jakarta.persistence.*;
import lombok.Data;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Objects;

@Data
@Entity
@Table(name = "user_clicklog" )
public class UserClick {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "user_id")
    private String userId;

    @Column(name = "clicknews_id")
    private String clickNewsId;

    @Column(name = "pos_or_neg")
    private Integer posOrNeg;  // 1 正向, 0 负向

    @Column(name = "click_time")
    private LocalDateTime clickTime;

    @Column(name = "dwell_time")
    private Integer dwellTime;

}
