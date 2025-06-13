-- 适配 unpack/table.unpack
local unpack = unpack or table.unpack

local cates_cnt = #ARGV - 1
local topn      = tonumber(ARGV[#ARGV])

-- 用随机 key 做中转
local tmp_key = 'tmp_union_' .. tostring(math.random(99999999))

-- 构造 KEYS/ARGV 的 {x, x, ...} 子数组传给 unpack
local zset_keys = {}
local weights   = {}

for i = 1, cates_cnt do
    table.insert(zset_keys, KEYS[i])
    table.insert(weights, ARGV[i])
end

-- 拼成一个参数全表
local args = {tmp_key, cates_cnt}
for i = 1, #zset_keys do
    table.insert(args, zset_keys[i])
end
table.insert(args, 'WEIGHTS')
for i = 1, #weights do
    table.insert(args, weights[i])
end

-- ZUNIONSTORE 调用必须所有参数平铺
redis.call('ZUNIONSTORE', unpack(args))

-- 取多一点做后续过滤
local candidates = redis.call('ZREVRANGE', tmp_key, 0, topn * 5 - 1)

local seen_key    = KEYS[cates_cnt + 1]
local dislike_key = KEYS[cates_cnt + 2]

local result = {}
for i = 1, #candidates do
    local nid = candidates[i]
    if redis.call('SISMEMBER', seen_key, nid) == 0 and
       redis.call('SISMEMBER', dislike_key, nid) == 0 then
        table.insert(result, nid)
    end
    if #result >= topn then break end
end

redis.call('DEL', tmp_key)
return result
