-- Single Redis primary: all validation, reservation and durable outbox append in one script.
-- Validate types before mutation because Redis scripts do not roll back runtime errors.
local expected={'hash','string','hash','hash','hash','stream','string'}
for i=1,7 do
 local ty=redis.call('TYPE',KEYS[i]).ok
 if ty~='none' and ty~=expected[i] then return redis.error_reply('WRONGTYPE claim key '..i) end
end
local old=redis.call('HGET',KEYS[4],ARGV[1])
if old then return {0,old} end
if redis.call('EXISTS',KEYS[1])==0 or redis.call('EXISTS',KEYS[2])==0 then return {5,'not warmed'} end
local now=tonumber(redis.call('TIME')[1])
if redis.call('HGET',KEYS[1],'status')~='active' then return {4,'inactive'} end
if now<tonumber(redis.call('HGET',KEYS[1],'starts_at')) or now>=tonumber(redis.call('HGET',KEYS[1],'ends_at')) then return {4,'outside activity'} end
if tonumber(redis.call('GET',KEYS[2]))<=0 then return {2,'sold out'} end
local count=tonumber(redis.call('HGET',KEYS[3],ARGV[2]) or '0')
if count>=tonumber(redis.call('HGET',KEYS[1],'per_user')) then return {3,'user limit'} end
-- Preserve full int64 merchant/user IDs; cjson decode/encode would round IDs above 2^53.
local payload=string.gsub(ARGV[3],'"accepted_at":0','"accepted_at":'..now,1)
redis.call('DECR',KEYS[2]); redis.call('HINCRBY',KEYS[3],ARGV[2],1)
redis.call('HSET',KEYS[4],ARGV[1],payload); redis.call('HSET',KEYS[5],ARGV[1],'pending')
redis.call('XADD',KEYS[6],'*','event',payload)
redis.call('INCR',KEYS[7])
return {0,payload}
