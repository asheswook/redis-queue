package redis_queue

import "github.com/redis/go-redis/v9"

var push = redis.NewScript(`
redis.replicate_commands()

local queue_name = KEYS[1]
local value = ARGV[1]
local result = redis.pcall('RPUSH', queue_name, value)

if tonumber(result) < 1 then
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

local ttl = tonumber(ARGV[1]) * 1000000
local time = redis.pcall('TIME')
local timestamp = tonumber(time[1]) * 1000000 + tonumber(time[2])

-- 여기에 ack zset에서 ttl이 지난 항목 1개를 가져온다
local expired_ack = redis.pcall('ZRANGEBYSCORE', ack_zset_name, 0, (timestamp - ttl), 'LIMIT', 0, 1)

-- ack zset에 있다면, ack zset에서 받아온 후, timestamp 최신화
if expired_ack and #expired_ack > 0 then
	local result = redis.pcall('ZADD', ack_zset_name, timestamp, expired_ack[1])
	if tonumber(result) ~= 0 then
		return { ERR = 'timestamp update failed' }
	end
	
	return expired_ack[1]
end

-- ack zset에 없다면, 메인 큐에서 POP
local result = redis.pcall('LPOP', queue_name)

if not result or #result == 0 then
    return false
end

-- 메인 큐에서 받아온 후, ack zset에 추가
local ack_result = redis.pcall('ZADD', ack_zset_name, 'NX', timestamp, result)

-- 중복 항목 없을 시
if tonumber(ack_result) == 1 then
	return result
end

-- 중복 항목 있을 경우, 기존 행위 롤백
redis.pcall('LPUSH', queue_name, result)

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
