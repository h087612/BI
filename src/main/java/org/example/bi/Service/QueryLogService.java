package org.example.bi.Service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class QueryLogService {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    public void log(String sqlText) {
        String insertSql = "INSERT INTO query_log (sql_text) VALUES (?)";
        jdbcTemplate.update(insertSql, sqlText);
    }
}
