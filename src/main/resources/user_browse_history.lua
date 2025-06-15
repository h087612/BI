-- 获取用户浏览历史（兴趣分类）
local userId = ARGV[1]
local page = tonumber(ARGV[2])
local pageSize = tonumber(ARGV[3])

-- 计算分页参数
local start = (page - 1) * pageSize
local stop = start + pageSize - 1

-- 获取用户分类评分
local userCateScoreKey = "user_cate_score:" .. userId
local categoryScores = redis.call('ZREVRANGE', userCateScoreKey, start, stop, 'WITHSCORES')

-- 获取总数
local total = redis.call('ZCARD', userCateScoreKey)

-- 构建返回结果
local result = {}
result["total"] = total
result["items"] = {}

-- 处理查询结果
for i = 1, #categoryScores, 2 do
    local item = {}
    item["category"] = categoryScores[i]
    item["score"] = tonumber(categoryScores[i + 1])
    table.insert(result["items"], item)
end

return cjson.encode(result)