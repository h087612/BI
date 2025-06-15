-- Lua script for efficient daily statistics aggregation
-- KEYS[1] = date string (yyyyMMdd)
-- ARGV[1] = limit (default 20)

local date = KEYS[1]
local limit = tonumber(ARGV[1]) or 20

-- Get news stats from daily ranking
local totalKey = "news_hot_rank_daily:" .. date
local newsStats = redis.call('ZREVRANGE', totalKey, 0, limit - 1, 'WITHSCORES')

-- Calculate total clicks for the day
local allNews = redis.call('ZRANGE', totalKey, 0, -1, 'WITHSCORES')
local totalClicks = 0

for i = 2, #allNews, 2 do
    totalClicks = totalClicks + tonumber(allNews[i])
end

-- Format result
local result = {}
result['totalClicks'] = totalClicks
result['newsStats'] = {}

for i = 1, #newsStats, 2 do
    table.insert(result['newsStats'], {
        newsId = newsStats[i],
        clickCount = tonumber(newsStats[i + 1])
    })
end

return cjson.encode(result)