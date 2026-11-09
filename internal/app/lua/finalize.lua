local state=redis.call('HGET',KEYS[1],ARGV[1])
if not state then return 0 end
if state=='issued' or state=='failed' then return 0 end
if ARGV[2]=='failed' then
 redis.call('INCR',KEYS[2]); redis.call('HINCRBY',KEYS[3],ARGV[3],-1)
end
redis.call('HSET',KEYS[1],ARGV[1],ARGV[2]); redis.call('INCR',KEYS[4]); return 1
