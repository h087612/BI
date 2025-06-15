-- 新闻统计批量查询Lua脚本
-- 用于优化Redis查询性能，减少网络往返

local start_date = KEYS[1]
local end_date = KEYS[2]
local limit = tonumber(ARGV[1]) or 20

-- 解析日期范围
local function parse_date(date_str)
    local year, month, day = date_str:match("(%d+)-(%d+)-(%d+)")
    return tonumber(year), tonumber(month), tonumber(day)
end

-- 生成日期范围内的所有日期
local function generate_date_range(start_str, end_str)
    local dates = {}
    local sy, sm, sd = parse_date(start_str)
    local ey, em, ed = parse_date(end_str)
    
    local start_time = os.time({year=sy, month=sm, day=sd})
    local end_time = os.time({year=ey, month=em, day=ed})
    
    for time = start_time, end_time, 86400 do
        local date = os.date("%Y%m%d", time)
        table.insert(dates, date)
    end
    
    return dates
end

-- 主逻辑
local dates = generate_date_range(start_date, end_date)
local news_clicks = {}
local total_clicks = 0

-- 聚合所有日期的数据
for _, date in ipairs(dates) do
    local key = "news_hot_rank_daily:" .. date
    local news_list = redis.call('ZREVRANGE', key, 0, -1, 'WITHSCORES')
    
    for i = 1, #news_list, 2 do
        local news_id = news_list[i]
        local clicks = tonumber(news_list[i + 1])
        
        if news_clicks[news_id] then
            news_clicks[news_id] = news_clicks[news_id] + clicks
        else
            news_clicks[news_id] = clicks
        end
        
        total_clicks = total_clicks + clicks
    end
end

-- 转换为数组并排序
local news_array = {}
for news_id, clicks in pairs(news_clicks) do
    table.insert(news_array, {news_id, clicks})
end

table.sort(news_array, function(a, b) return a[2] > b[2] end)

-- 获取TOP N
local top_news = {}
for i = 1, math.min(limit, #news_array) do
    table.insert(top_news, news_array[i][1])
    table.insert(top_news, tostring(news_array[i][2]))
end

-- 返回结果
return cjson.encode({
    totalClicks = total_clicks,
    newsStats = top_news
})