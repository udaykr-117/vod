package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisStore struct {
	client *redis.Client
}

func NewRedisStore(addr string) *RedisStore {
	return &RedisStore{client: redis.NewClient(&redis.Options{Addr: addr})}
}

func (s *RedisStore) key(uploadID string) string {
	return fmt.Sprintf("upload:%s:session", uploadID)
}

func (s *RedisStore) Create(ctx context.Context, sess UploadSession, ttl time.Duration) error {
	data, err := json.Marshal(sess)
	if err != nil {
		return fmt.Errorf("marshal session: %w", err)
	}
	if err := s.client.Set(ctx, s.key(sess.UploadID), data, ttl).Err(); err != nil {
		return fmt.Errorf("redis set: %w", err)
	}
	return nil
}

func (s *RedisStore) Get(ctx context.Context, uploadID string) (UploadSession, error) {
	data, err := s.client.Get(ctx, s.key(uploadID)).Bytes()
	if errors.Is(err, redis.Nil) {
		return UploadSession{}, ErrNotFound
	}
	if err != nil {
		return UploadSession{}, fmt.Errorf("redis get: %w", err)
	}
	var sess UploadSession
	if err := json.Unmarshal(data, &sess); err != nil {
		return UploadSession{}, fmt.Errorf("unmarshal session: %w", err)
	}
	return sess, nil
}

func (s *RedisStore) Delete(ctx context.Context, uploadID string) error {
	if err := s.client.Del(ctx, s.key(uploadID)).Err(); err != nil {
		return fmt.Errorf("redis del: %w", err)
	}
	return nil
}

func (s *RedisStore) Ping(ctx context.Context) error {
	return s.client.Ping(ctx).Err()
}
