-- Lua script for batch user statistics retrieval
-- KEYS[1..n] = user IDs
-- ARGV[1] = operation type ("seen" or "category")

local operation = ARGV[1]
local results = {}

for i, userId in ipairs(KEYS) do
    if operation == "seen" then
        local seenKey = "user_seen_news:" .. userId
        local count = redis.call('SCARD', seenKey)
        table.insert(results, {userId = userId, clickCount = count})
    elseif operation == "category" then
        local cateKey = "user_cate_score:" .. userId
        local scores = redis.call('HGETALL', cateKey)
        table.insert(results, {userId = userId, categories = scores})
    end
end

return cjson.encode(results)