if redis.call('EXISTS',KEYS[1])==1 then return 0 end
-- An existing reservation ledger without metadata must never be reset.
if redis.call('EXISTS',KEYS[3])==1 then return -1 end
redis.call('HSET',KEYS[1],'status',ARGV[1],'starts_at',ARGV[2],'ends_at',ARGV[3],'per_user',ARGV[4])
redis.call('SET',KEYS[2],ARGV[5]); return 1
