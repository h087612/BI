-- get_news_popularity.lua
-- 输入：
-- KEYS: { "news_hot_rank_daily:20250601", "news_hot_rank_daily:20250602", ... }
-- ARGV[1]: news_id
-- 返回：
-- List: { score1, score2, ... }，仅包含非零点击量的分数，顺序与 KEYS 一致

local results = {}
local news_id = ARGV[1]
local index = 1

-- 检查 news_id 是否有效
if not news_id or news_id == "" then
    return results
end

for i, key in ipairs(KEYS) do
    local score = redis.call('ZSCORE', key, news_id)
    if score and tonumber(score) > 0 then
        results[index] = score
        index = index + 1
    end
end

return results