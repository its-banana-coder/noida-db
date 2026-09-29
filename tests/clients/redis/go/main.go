// go-redis against noida-db: what Go applications do. go-redis v9 speaks
// RESP3 by default, so this also covers that path; the scenario runs once per
// protocol. Run through tests/clients/redis/run.sh, which starts the server
// and sets NOIDA_REDIS_PORT.
package main

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	failures []string
	checks   int
	ctx      = context.Background()
)

func check(name string, got, want interface{}) {
	checks++
	if !reflect.DeepEqual(got, want) {
		failures = append(failures, fmt.Sprintf("%s: got %#v, want %#v", name, got, want))
	}
}

func scenario(port string, protocol int) {
	tag := fmt.Sprintf("resp%d", protocol)
	r := redis.NewClient(&redis.Options{Addr: "127.0.0.1:" + port, Protocol: protocol})
	defer r.Close()
	r.FlushAll(ctx)

	check(tag+" ping", r.Ping(ctx).Val(), "PONG")

	// strings
	check(tag+" set ex", r.Set(ctx, "k", "v", 100*time.Second).Val(), "OK")
	check(tag+" get", r.Get(ctx, "k").Val(), "v")
	ttl := r.TTL(ctx, "k").Val()
	check(tag+" ttl", ttl > 0 && ttl <= 100*time.Second, true)
	check(tag+" setnx", r.SetNX(ctx, "k", "w", 0).Val(), false)
	check(tag+" incrby", r.IncrBy(ctx, "n", 5).Val(), int64(5))
	check(tag+" incrbyfloat", r.IncrByFloat(ctx, "f", 1.5).Val(), 1.5)
	check(tag+" mset/mget", func() interface{} {
		r.MSet(ctx, "a", 1, "b", 2)
		return r.MGet(ctx, "a", "b", "zz").Val()
	}(), []interface{}{"1", "2", nil})
	_, err := r.Get(ctx, "missing").Result()
	check(tag+" nil error", err, redis.Nil)

	// hashes, lists, sets, sorted sets
	r.HSet(ctx, "h", map[string]interface{}{"x": "1", "y": "2"})
	check(tag+" hgetall", r.HGetAll(ctx, "h").Val(), map[string]string{"x": "1", "y": "2"})
	check(tag+" hincrby", r.HIncrBy(ctx, "h", "x", 4).Val(), int64(5))
	r.RPush(ctx, "l", "a", "b", "c")
	check(tag+" lrange", r.LRange(ctx, "l", 0, -1).Val(), []string{"a", "b", "c"})
	check(tag+" lpop count", r.LPopCount(ctx, "l", 2).Val(), []string{"a", "b"})
	r.SAdd(ctx, "s", "x", "y", "z")
	members := r.SMembers(ctx, "s").Val()
	sort.Strings(members)
	check(tag+" smembers", members, []string{"x", "y", "z"})
	r.ZAdd(ctx, "z", redis.Z{Score: 1.5, Member: "a"}, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"})
	check(tag+" zrange withscores", r.ZRangeWithScores(ctx, "z", 0, -1).Val(),
		[]redis.Z{{Score: 1.5, Member: "a"}, {Score: 2, Member: "b"}, {Score: 3, Member: "c"}})
	check(tag+" zscore", r.ZScore(ctx, "z", "b").Val(), 2.0)
	check(tag+" zrangebyscore", r.ZRangeByScore(ctx, "z", &redis.ZRangeBy{Min: "2", Max: "3"}).Val(), []string{"b", "c"})
	check(tag+" zincrby", r.ZIncrBy(ctx, "z", 0.5, "a").Val(), 2.0)

	// keys and scan
	for i := 0; i < 30; i++ {
		r.Set(ctx, "scan:"+strconv.Itoa(i), i, 0)
	}
	count := 0
	iter := r.Scan(ctx, 0, "scan:*", 7).Iterator()
	for iter.Next(ctx) {
		count++
	}
	check(tag+" scan iterator", count, 30)
	check(tag+" type", r.Type(ctx, "h").Val(), "hash")
	check(tag+" rename", r.Rename(ctx, "scan:0", "moved").Val(), "OK")

	// pipelines and transactions
	pipe := r.Pipeline()
	set := pipe.Set(ctx, "p1", 1, 0)
	inc := pipe.Incr(ctx, "p1")
	get := pipe.Get(ctx, "p1")
	_, err = pipe.Exec(ctx)
	check(tag+" pipeline", []interface{}{err, set.Val(), inc.Val(), get.Val()}, []interface{}{nil, "OK", int64(2), "2"})
	var incr *redis.IntCmd
	_, err = r.TxPipelined(ctx, func(p redis.Pipeliner) error {
		incr = p.Incr(ctx, "t1")
		p.Expire(ctx, "t1", time.Minute)
		return nil
	})
	check(tag+" tx pipelined", []interface{}{err, incr.Val()}, []interface{}{nil, int64(1)})

	// optimistic locking with WATCH
	r.Set(ctx, "watched", 1, 0)
	err = r.Watch(ctx, func(tx *redis.Tx) error {
		n, err := tx.Get(ctx, "watched").Int()
		if err != nil {
			return err
		}
		_, err = tx.TxPipelined(ctx, func(p redis.Pipeliner) error {
			p.Set(ctx, "watched", n+1, 0)
			return nil
		})
		return err
	}, "watched")
	check(tag+" watch", []interface{}{err, r.Get(ctx, "watched").Val()}, []interface{}{nil, "2"})
	// a conflicting write makes the transaction fail with TxFailedErr
	err = r.Watch(ctx, func(tx *redis.Tx) error {
		r.Set(ctx, "watched", 100, 0)
		_, err := tx.TxPipelined(ctx, func(p redis.Pipeliner) error {
			p.Set(ctx, "watched", 3, 0)
			return nil
		})
		return err
	}, "watched")
	check(tag+" watch conflict", err, redis.TxFailedErr)

	// Lua
	script := redis.NewScript("return redis.call('incrby', KEYS[1], ARGV[1])")
	first, _ := script.Run(ctx, r, []string{"lua"}, 3).Int()
	second, _ := script.Run(ctx, r, []string{"lua"}, 4).Int()
	check(tag+" script", []int{first, second}, []int{3, 7})
	check(tag+" eval table", r.Eval(ctx, "return {1, 'two', {3}}", nil).Val(),
		[]interface{}{int64(1), "two", []interface{}{int64(3)}})
	// EVALSHA after SCRIPT LOAD, and the fallback go-redis uses on NOSCRIPT
	sha := r.ScriptLoad(ctx, "return 42").Val()
	check(tag+" evalsha", r.EvalSha(ctx, sha, nil).Val(), int64(42))

	// pub/sub
	sub := r.Subscribe(ctx, "chan")
	_, err = sub.Receive(ctx)
	check(tag+" subscribe confirmation", err, nil)
	var wg sync.WaitGroup
	var got string
	wg.Add(1)
	go func() {
		defer wg.Done()
		m, err := sub.ReceiveTimeout(ctx, 3*time.Second)
		if err == nil {
			if msg, ok := m.(*redis.Message); ok {
				got = msg.Payload
			}
		}
	}()
	time.Sleep(200 * time.Millisecond)
	check(tag+" publish reaches a subscriber", r.Publish(ctx, "chan", "hello").Val(), int64(1))
	wg.Wait()
	check(tag+" pubsub message", got, "hello")
	sub.Close()

	// streams
	id := r.XAdd(ctx, &redis.XAddArgs{Stream: "stream", Values: map[string]interface{}{"f": "v"}}).Val()
	msgs := r.XRange(ctx, "stream", "-", "+").Val()
	check(tag+" xrange", len(msgs) == 1 && msgs[0].Values["f"] == "v", true)
	r.XGroupCreate(ctx, "stream", "g", "0")
	read := r.XReadGroup(ctx, &redis.XReadGroupArgs{Group: "g", Consumer: "c1", Streams: []string{"stream", ">"}, Count: 1}).Val()
	check(tag+" xreadgroup", len(read) == 1 && read[0].Messages[0].ID == id, true)
	check(tag+" xack", r.XAck(ctx, "stream", "g", id).Val(), int64(1))

	// other types
	check(tag+" pfadd/pfcount", []int64{r.PFAdd(ctx, "hll", "a", "b", "c").Val(), r.PFCount(ctx, "hll").Val()}, []int64{1, 3})
	r.RPush(ctx, "nums", 3, 1, 2)
	check(tag+" sort", r.Sort(ctx, "nums", &redis.Sort{}).Val(), []string{"1", "2", "3"})
	r.GeoAdd(ctx, "geo", &redis.GeoLocation{Name: "Palermo", Longitude: 13.361389, Latitude: 38.115556},
		&redis.GeoLocation{Name: "Catania", Longitude: 15.087269, Latitude: 37.502669})
	check(tag+" geodist", int(r.GeoDist(ctx, "geo", "Palermo", "Catania", "km").Val()), 166)
	r.SetBit(ctx, "bits", 7, 1)
	check(tag+" bitcount", r.BitCount(ctx, "bits", nil).Val(), int64(1))

	// errors
	r.Set(ctx, "str", "text", 0)
	e := r.Incr(ctx, "str").Err()
	check(tag+" error text", e != nil && len(e.Error()) > 0 && contains(e.Error(), "not an integer"), true)
	e = r.LPush(ctx, "str", "x").Err()
	check(tag+" wrongtype", e != nil && contains(e.Error(), "WRONGTYPE"), true)

	// binary safety
	blob := make([]byte, 256)
	for i := range blob {
		blob[i] = byte(i)
	}
	r.Set(ctx, "blob", blob, 0)
	back, _ := r.Get(ctx, "blob").Bytes()
	check(tag+" binary round trip", string(back) == string(blob), true)

	// blocking pop served by another connection
	other := redis.NewClient(&redis.Options{Addr: "127.0.0.1:" + port, Protocol: protocol})
	defer other.Close()
	res := make(chan []string, 1)
	go func() { res <- r.BLPop(ctx, 3*time.Second, "queue").Val() }()
	time.Sleep(300 * time.Millisecond)
	other.RPush(ctx, "queue", "job")
	check(tag+" blpop", <-res, []string{"queue", "job"})
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func main() {
	port := os.Getenv("NOIDA_REDIS_PORT")
	for _, proto := range []int{2, 3} {
		scenario(port, proto)
	}
	fmt.Printf("go-redis: %d checks, %d failed\n", checks, len(failures))
	for _, f := range failures {
		fmt.Println("  FAIL", f)
	}
	if len(failures) > 0 {
		os.Exit(1)
	}
}
