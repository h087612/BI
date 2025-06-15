-- 优化 /news 接口的数据库索引

-- 1. 为发布时间创建索引（用于排序）
CREATE INDEX idx_publish_time ON static_news(publish_time DESC);

-- 2. 创建复合索引（category + topic + publish_time）
CREATE INDEX idx_category_topic_time ON static_news(category, topic, publish_time DESC);

-- 3. 为headline创建全文索引（如果还没有）
ALTER TABLE static_news ADD FULLTEXT INDEX idx_headline_fulltext (headline);

-- 4. 查看表的索引情况
SHOW INDEX FROM static_news;

-- 5. 分析表以更新统计信息
ANALYZE TABLE static_news;