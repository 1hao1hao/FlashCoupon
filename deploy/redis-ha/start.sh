#!/bin/sh
set -eu
# Redis rewrites these files after promotion; initialize only once per volume.
for secret in "$REDIS_PASSWORD" "$REDIS_SENTINEL_PASSWORD"; do
 case "$secret" in ''|*[!A-Za-z0-9_-]*) echo 'Redis HA secrets must use letters, digits, underscore or dash' >&2; exit 1;; esac
done
mode="$1"; node="$2"
prefix="${REDIS_HA_PREFIX:-172.29.239}"
case "$prefix" in ''|*[!0-9.]*) echo 'invalid HA private address prefix' >&2; exit 1;; esac
case "$node" in
 redis) announce="$prefix.11";; redis-replica-1) announce="$prefix.12";; redis-replica-2) announce="$prefix.13";;
 sentinel-1) announce="$prefix.21";; sentinel-2) announce="$prefix.22";; sentinel-3) announce="$prefix.23";;
 *) exit 1;;
esac
if [ "$mode" = sentinel ]; then
 if [ ! -s /data/sentinel.conf ]; then
  cat > /data/sentinel.conf <<EOF
port 26379
bind 0.0.0.0
protected-mode yes
dir /data
requirepass $REDIS_SENTINEL_PASSWORD
sentinel resolve-hostnames yes
sentinel announce-hostnames yes
sentinel announce-ip $announce
sentinel announce-port 26379
sentinel monitor flashcoupon $prefix.11 6379 2
sentinel auth-pass flashcoupon $REDIS_PASSWORD
sentinel down-after-milliseconds flashcoupon 2000
sentinel failover-timeout flashcoupon 15000
sentinel parallel-syncs flashcoupon 1
EOF
 fi
 # Preserve epochs/current master when upgrading an existing hostname-based lab.
 for verb in monitor known-replica known-slave; do
  sed -i "s/^sentinel $verb flashcoupon redis-replica-1 /sentinel $verb flashcoupon $prefix.12 /; s/^sentinel $verb flashcoupon redis-replica-2 /sentinel $verb flashcoupon $prefix.13 /; s/^sentinel $verb flashcoupon redis /sentinel $verb flashcoupon $prefix.11 /" /data/sentinel.conf
 done
 sed -i "s/^sentinel announce-ip .*/sentinel announce-ip $announce/; s/^sentinel known-sentinel flashcoupon sentinel-1 /sentinel known-sentinel flashcoupon $prefix.21 /; s/^sentinel known-sentinel flashcoupon sentinel-2 /sentinel known-sentinel flashcoupon $prefix.22 /; s/^sentinel known-sentinel flashcoupon sentinel-3 /sentinel known-sentinel flashcoupon $prefix.23 /" /data/sentinel.conf
 chown redis:redis /data /data/sentinel.conf
 exec /usr/local/bin/docker-entrypoint.sh redis-server /data/sentinel.conf --sentinel
fi
if [ ! -s /data/redis.conf ]; then
 cat > /data/redis.conf <<EOF
bind 0.0.0.0
port 6379
protected-mode yes
dir /data
appendonly yes
appendfsync always
maxmemory-policy noeviction
requirepass $REDIS_PASSWORD
masterauth $REDIS_PASSWORD
replica-announce-ip $announce
replica-announce-port 6379
min-replicas-to-write 1
min-replicas-max-lag 2
EOF
 if [ "$node" != redis ]; then echo "replicaof $prefix.11 6379" >> /data/redis.conf; fi
fi
sed -i "s/^replica-announce-ip .*/replica-announce-ip $announce/; s/^replicaof redis-replica-1 /replicaof $prefix.12 /; s/^replicaof redis-replica-2 /replicaof $prefix.13 /; s/^replicaof redis /replicaof $prefix.11 /" /data/redis.conf
chown redis:redis /data /data/redis.conf
exec /usr/local/bin/docker-entrypoint.sh redis-server /data/redis.conf
