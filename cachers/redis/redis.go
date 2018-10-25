package redis

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/opencensus-integrations/redigo/redis"

	"github.com/qedus/nds/v2"
)

const (
	// Datastore max size is 1,048,572 bytes (1 MiB - 4 bytes)
	// + 4 bytes for uint32 flags
	maxCacheSize = (1 << 20)

	casScript = `local exp = tonumber(ARGV[3])
	local orig = redis.call("get", KEYS[1])
	if not orig then
		return nil
	end
	if orig == ARGV[1]
	then
		if exp >= 0
		then
			return redis.call("SET", KEYS[1], ARGV[2], "PX", exp)
		else
			return redis.call("SET", KEYS[1], ARGV[2])
		end
	else
		return redis.error_reply("cas conflict")
	end`
)

var (
	casSha = ""
)

// NewCacher will return a nds.Cacher backed by
// the provided redis pool. It will try and load a script
// into the redis script cache and return an error if it is
// unable to. Anytime the redis script cache is flushed, a new
// redis nds.Cacher must be initialized to reload the script.
func NewCacher(ctx context.Context, pool *redis.Pool) (nds.Cacher, error) {
	var err error
	conn := pool.GetWithContext(ctx).(redis.ConnWithContext)
	defer conn.CloseContext(ctx)
	if casSha, err = redis.String(conn.DoContext(ctx, "SCRIPT", "LOAD", casScript)); err != nil {
		return nil, err
	}
	return &backend{store: pool}, nil
}

type backend struct {
	store *redis.Pool
}

var bufPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func (b *backend) NewContext(c context.Context) (context.Context, error) {
	return c, nil
}

func (b *backend) AddMulti(ctx context.Context, items []*nds.Item) error {
	redisConn := b.store.GetWithContext(ctx).(redis.ConnWithContext)
	defer redisConn.CloseContext(ctx)

	return set(ctx, redisConn, true, items)
}

func set(ctx context.Context, conn redis.ConnWithContext, nx bool, items []*nds.Item) error {
	me := make(nds.MultiError, len(items))
	hasErr := false
	var flushErr error
	go func() {
		buf := bufPool.Get().(*bytes.Buffer)
		for i, item := range items {
			select {
			case <-ctx.Done():
				break
			default:
			}
			buf.Reset()
			buf.Grow(4 + len(item.Value))
			binary.Write(buf, binary.LittleEndian, item.Flags)
			buf.Write(item.Value)

			args := []interface{}{item.Key, buf.Bytes()}
			if nx {
				args = append(args, "NX")
			}

			if item.Expiration != 0 {
				expire := item.Expiration.Truncate(time.Millisecond) / time.Millisecond
				args = append(args, "PX", int64(expire))
			}

			if err := conn.SendContext(ctx, "SET", args...); err != nil {
				me[i] = err
			}
		}
		flushErr = conn.FlushContext(ctx)
		if buf.Cap() <= maxCacheSize {
			bufPool.Put(buf)
		}
	}()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		for i := 0; i < len(items); i++ {
			select {
			case <-ctx.Done():
				flushErr = ctx.Err()
				break
			default:
			}
			if flushErr != nil {
				break
			}
			if me[i] != nil {
				// We couldn't queue the command so don't expect it's response
				hasErr = true
				continue
			}
			if _, err := redis.String(conn.ReceiveContext(ctx)); err != nil {
				if nx && err == redis.ErrNil {
					me[i] = nds.ErrNotStored
				} else {
					me[i] = err
				}
				hasErr = true
			}
		}
	}()

	wg.Wait()

	if flushErr != nil {
		return flushErr
	}

	if hasErr {
		return me
	}
	return nil
}

func (b *backend) CompareAndSwapMulti(ctx context.Context, items []*nds.Item) error {
	redisConn := b.store.GetWithContext(ctx).(redis.ConnWithContext)
	defer redisConn.CloseContext(ctx)

	me := make(nds.MultiError, len(items))
	hasErr := false
	var flushErr error

	go func() {
		buf := bufPool.Get().(*bytes.Buffer)
		for i, item := range items {
			select {
			case <-ctx.Done():
				break
			default:
			}
			if cas, ok := item.GetCASInfo().([]byte); ok && cas != nil {
				buf.Reset()
				buf.Grow(4 + len(item.Value))
				binary.Write(buf, binary.LittleEndian, item.Flags)
				buf.Write(item.Value)
				expire := int64(item.Expiration.Truncate(time.Millisecond) / time.Millisecond)
				if item.Expiration == 0 {
					expire = -1
				}
				if err := redisConn.SendContext(ctx, "EVALSHA", casSha, "1", item.Key, cas, buf.Bytes(), expire); err != nil {
					me[i] = err
				}
			} else {
				me[i] = nds.ErrNotStored
			}
		}
		flushErr = redisConn.FlushContext(ctx)
		if buf.Cap() <= maxCacheSize {
			bufPool.Put(buf)
		}
	}()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		for i := 0; i < len(items); i++ {
			select {
			case <-ctx.Done():
				flushErr = ctx.Err()
				break
			default:
			}
			if flushErr != nil {
				break
			}
			if me[i] != nil {
				// We couldn't queue the command so don't expect it's response
				hasErr = true
				continue
			}
			if _, err := redis.String(redisConn.ReceiveContext(ctx)); err != nil {
				if err == redis.ErrNil {
					me[i] = nds.ErrNotStored
				} else if err.Error() == "cas conflict" {
					me[i] = nds.ErrCASConflict
				} else {
					me[i] = err
				}
				hasErr = true
			}
		}
	}()

	wg.Wait()

	if flushErr != nil {
		return flushErr
	}

	if hasErr {
		return me
	}
	return nil
}

func (b *backend) DeleteMulti(ctx context.Context, keys []string) error {
	redisConn := b.store.GetWithContext(ctx).(redis.ConnWithContext)
	defer redisConn.CloseContext(ctx)

	if len(keys) == 0 {
		return nil
	}

	args := make([]interface{}, len(keys))
	for i, key := range keys {
		args[i] = key
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if num, err := redis.Int64(redisConn.DoContext(ctx, "DEL", args...)); err != nil {
		return err
	} else if num != int64(len(keys)) {
		return fmt.Errorf("redis: expected to remove %d keys, but only removed %d", len(keys), num)
	}
	return nil
}

func (b *backend) GetMulti(ctx context.Context, keys []string) (map[string]*nds.Item, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	redisConn := b.store.GetWithContext(ctx).(redis.ConnWithContext)
	defer redisConn.CloseContext(ctx)

	args := make([]interface{}, len(keys))
	for i, key := range keys {
		args[i] = key
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	cachedItems, err := redis.ByteSlices(redisConn.DoContext(ctx, "MGET", args...))
	if err != nil {
		return nil, err
	}

	result := make(map[string]*nds.Item)
	me := make(nds.MultiError, len(keys))
	hasErr := false
	if len(cachedItems) != len(keys) {
		return nil, fmt.Errorf("redis: len(cachedItems) != len(keys) (%d != %d)", len(cachedItems), len(keys))
	}
	for i, key := range keys {
		if cacheItem := cachedItems[i]; cacheItem != nil {
			if got := len(cacheItem); got < 4 {
				me[i] = fmt.Errorf("redis: cached item should be atleast 4 bytes, got %d", got)
				hasErr = true
				continue
			}
			buf := bytes.NewBuffer(cacheItem)
			var flags uint32
			if err = binary.Read(buf, binary.LittleEndian, &flags); err != nil {
				me[i] = err
				hasErr = true
				continue
			}
			ndsItem := &nds.Item{
				Key:   key,
				Flags: flags,
				Value: buf.Bytes(),
			}

			// Keep a copy of the original value data for any future CAS operations
			ndsItem.SetCASInfo(append([]byte(nil), cacheItem...))
			result[key] = ndsItem
		}
	}
	if hasErr {
		return result, me
	}

	return result, nil
}

func (b *backend) SetMulti(ctx context.Context, items []*nds.Item) error {
	redisConn := b.store.GetWithContext(ctx).(redis.ConnWithContext)
	defer redisConn.CloseContext(ctx)

	return set(ctx, redisConn, false, items)
}
