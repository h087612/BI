-- 高级新闻统计Lua脚本
-- 支持类别过滤和用户喜好过滤的组合

-- 参数说明：
-- KEYS[1]: 开始日期
-- KEYS[2]: 结束日期  
-- KEYS[3]: 用户ID (可选)
-- ARGV[1]: 限制数量
-- ARGV[2]: 过滤类型 (like/dislike/all)
-- ARGV[3]: 类别 (可选)

local start_date = KEYS[1]
local end_date = KEYS[2]
local user_id = KEYS[3]
local limit = tonumber(ARGV[1]) or 20
local filter_type = ARGV[2] or "all"
local category = ARGV[3]

-- 生成日期范围
local function generate_dates(start_str, end_str)
    local dates = {}
    local start_num = tonumber(start_str)
    local end_num = tonumber(end_str)
    
    for date = start_num, end_num do
        table.insert(dates, tostring(date))
    end
    
    return dates
end

-- 主逻辑
local dates = generate_dates(start_date, end_date)
local news_clicks = {}
local total_clicks = 0

-- 获取用户过滤集合
local filter_set = nil
if user_id and user_id ~= "" then
    if filter_type == "like" then
        filter_set = "user_seen_news:" .. user_id
    elseif filter_type == "dislike" then
        filter_set = "user_dislike_news:" .. user_id
    end
end

-- 聚合数据
for _, date in ipairs(dates) do
    local key
    if category and category ~= "" then
        -- 如果指定了类别，使用类别日榜
        key = "news_hot_rank_daily:" .. category .. ":" .. date
    else
        -- 使用总榜
        key = "news_hot_rank_daily:" .. date
    end
    
    local news_list = redis.call('ZREVRANGE', key, 0, -1, 'WITHSCORES')
    
    for i = 1, #news_list, 2 do
        local news_id = news_list[i]
        local clicks = tonumber(news_list[i + 1])
        
        -- 检查用户过滤
        local should_include = true
        if filter_set then
            should_include = redis.call('SISMEMBER', filter_set, news_id) == 1
        end
        
        if should_include then
            if news_clicks[news_id] then
                news_clicks[news_id] = news_clicks[news_id] + clicks
            else
                news_clicks[news_id] = clicks
            end
            total_clicks = total_clicks + clicks
        end
    end
end

-- 排序和限制
local news_array = {}
for news_id, clicks in pairs(news_clicks) do
    table.insert(news_array, {news_id, clicks})
end

table.sort(news_array, function(a, b) return a[2] > b[2] end)

-- 构建结果
local top_news = {}
for i = 1, math.min(limit, #news_array) do
    table.insert(top_news, {
        newsId = news_array[i][1],
        clickCount = news_array[i][2]
    })
end

return cjson.encode({
    totalClicks = total_clicks,
    newsStats = top_news
})