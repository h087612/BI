-- 用户过滤的新闻统计Lua脚本
-- 在Redis中原子操作：聚合时间范围内的新闻点击量，并根据用户喜好过滤

-- 参数说明：
-- KEYS[1]: 开始日期 (格式: 20190710)
-- KEYS[2]: 结束日期 (格式: 20190712)
-- KEYS[3]: 用户ID (可选)
-- ARGV[1]: 限制数量 (默认20)
-- ARGV[2]: 过滤类型 (like/dislike/all)

local start_date = KEYS[1]
local end_date = KEYS[2]
local user_id = KEYS[3]
local limit = tonumber(ARGV[1]) or 20
local filter_type = ARGV[2] or "all"

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

-- 获取用户喜好集合
local function get_user_filter_set(uid, ftype)
    if not uid or uid == "" then
        return nil
    end
    
    if ftype == "like" then
        -- 用户看过的新闻（pos_or_neg=1）
        return "user_seen_news:" .. uid
    elseif ftype == "dislike" then
        -- 用户不喜欢的新闻（pos_or_neg=0）
        return "user_dislike_news:" .. uid
    end
    
    return nil
end

-- 主逻辑
local dates = generate_dates(start_date, end_date)
local news_clicks = {}
local total_clicks = 0

-- 获取用户过滤集合
local filter_set = get_user_filter_set(user_id, filter_type)

-- 聚合所有日期的数据
for _, date in ipairs(dates) do
    local key = "news_hot_rank_daily:" .. date
    local news_list = redis.call('ZREVRANGE', key, 0, -1, 'WITHSCORES')
    
    for i = 1, #news_list, 2 do
        local news_id = news_list[i]
        local clicks = tonumber(news_list[i + 1])
        
        -- 检查是否需要过滤
        local should_include = true
        if filter_set then
            if filter_type == "like" then
                -- 只包含用户喜欢的新闻
                should_include = redis.call('SISMEMBER', filter_set, news_id) == 1
            elseif filter_type == "dislike" then
                -- 只包含用户不喜欢的新闻
                should_include = redis.call('SISMEMBER', filter_set, news_id) == 1
            end
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

-- 转换为数组并排序
local news_array = {}
for news_id, clicks in pairs(news_clicks) do
    table.insert(news_array, {news_id, clicks})
end

table.sort(news_array, function(a, b) return a[2] > b[2] end)

-- 获取TOP N
local top_news = {}
for i = 1, math.min(limit, #news_array) do
    table.insert(top_news, {
        newsId = news_array[i][1],
        clickCount = news_array[i][2]
    })
end

-- 返回JSON结果
return cjson.encode({
    totalClicks = total_clicks,
    newsStats = top_news
})