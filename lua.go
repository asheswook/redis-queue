package redis_queue

import "github.com/redis/go-redis/v9"

var push = redis.NewScript(`
redis.replicate_commands()

local queue_name = KEYS[1]
local value = ARGV[1]
local result = redis.pcall('RPUSH', queue_name, value)

if tonumber(result) ~= 1 then
	return false
end

return true
`)

var pop = redis.NewScript(`
redis.replicate_commands()

local queue_name = KEYS[1]
local result = redis.pcall('LPOP', queue_name)

if not result or #result == 0 then
	return false
end

return result
`)

var ackPop = redis.NewScript(`
redis.replicate_commands()

local queue_name = KEYS[1]
local ack_zset_name = KEYS[2]

local ttl = ARGV[1]
local timestamp = os.time(os.date("!*t"))

-- 여기에 ack zset에서 ttl이 지난 항목 1개를 가져온다
local expired_ack = redis.pcall('ZRANGEBYSCORE', ack_zset_name, 0, timestamp - ttl, 'LIMIT', 0, 1)

-- ack zset에 있다면, ack zset에서 받아온 후, timestamp 최신화
if expired_ack and #expired_ack > 0 then
	-- 앞에 붙은 숫자를 제거하고, |로 split하고, 1번째 인덱스를 가져옴
	local i, j = string.find(expired_ack[1], '|')
	local prefix = string.sub(expired_ack[1], 0, i-1)
	local result = string.sub(expired_ack[1], j+1)
    redis.pcall('ZREM', ack_zset_name, expired_ack[1])

	return { prefix, result }
end

-- ack zset에 없다면, 메인 큐에서 POP
local result = redis.pcall('LPOP', queue_name)

if not result or #result == 0 then
    return false
end

-- 메인 큐에서 받아온 후, ack zset에 추가
local ack_result = redis.pcall('ZADD', ack_zset_name, 'NX', timestamp, '0'..'|'..result)

-- 중복 항목 없을 시
if tonumber(ack_result) == 1 then
	return {'0'..'|', result}
end

-- 중복 항목 있을 시
local last_member = redis.pcall('ZREVRANGEBYSCORE', ack_zset_name, timestamp, timestamp, 'LIMIT', 0, 1)

-- last member를 |로 split하고, 0번째 인덱스를 가져옴
local i, j = string.find(last_member, '|')
local next_member_prefix = tonumber(string.sub(last_member, 0, i-1)) + 1

local ack_result = redis.pcall('ZADD', ack_zset_name, 'NX', timestamp, next_member_prefix..'|'..result)

if tonumber(ack_result) == 1 then
	return { next_member_prefix..'|', result }
end

return { ERR = 'EOF while ackPop' }
`)

// AckPop 후 성공적으로 처리했을 때, ack zset에서 삭제하는 스크립트
var ack = redis.NewScript(`
redis.replicate_commands()

local ack_zset_name = KEYS[1]
local value = ARGV[1]

local result = redis.pcall('ZREM', ack_zset_name, value)

if tonumber(result) == 1 then
	return true
end

return false
`)
