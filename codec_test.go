/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-06-16 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-06-16 17:15:15
 * @FilePath: \go-casbin-nats-adapter\codec_test.go
 * @Description: 二进制编解码单元测试（不依赖 NATS 服务器）
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */

package natsadapter

import (
	"encoding/binary"
	"encoding/json"
	"testing"
	"time"

	"github.com/kamalyes/go-casbin/policy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMarshalUnmarshalRoundTrip(t *testing.T) {
	original := &policy.ChangeEvent{
		ID:        "test-id-123",
		Type:      policy.EventTypePolicyAdded,
		PType:     "p",
		OldPolicy: []string{"alice", "data1", "read"},
		NewPolicy: []string{"bob", "data2", "write"},
		Source:    "node-test",
		Timestamp: time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC),
	}

	data, err := MarshalEvent(original)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	decoded, err := UnmarshalEvent(data)
	require.NoError(t, err)
	defer releaseEvent(decoded)

	assert.Equal(t, original.ID, decoded.ID)
	assert.Equal(t, original.Type, decoded.Type)
	assert.Equal(t, original.PType, decoded.PType)
	assert.Equal(t, original.OldPolicy, decoded.OldPolicy)
	assert.Equal(t, original.NewPolicy, decoded.NewPolicy)
	assert.Equal(t, original.Source, decoded.Source)
	assert.True(t, original.Timestamp.Equal(decoded.Timestamp))
}

func TestMarshalUnmarshalEmptyFields(t *testing.T) {
	original := &policy.ChangeEvent{
		ID:        "",
		Type:      policy.EventTypePolicyReload,
		PType:     "",
		OldPolicy: nil,
		NewPolicy: nil,
		Source:    "node-empty",
		Timestamp: time.Time{},
	}

	data, err := MarshalEvent(original)
	require.NoError(t, err)

	decoded, err := UnmarshalEvent(data)
	require.NoError(t, err)
	defer releaseEvent(decoded)

	assert.Equal(t, "", decoded.ID)
	assert.Equal(t, policy.EventTypePolicyReload, decoded.Type)
	assert.Equal(t, "", decoded.PType)
	assert.Nil(t, decoded.OldPolicy)
	assert.Nil(t, decoded.NewPolicy)
	assert.Equal(t, "node-empty", decoded.Source)
	assert.True(t, decoded.Timestamp.IsZero())
}

func TestMarshalUnmarshalSinglePolicy(t *testing.T) {
	original := &policy.ChangeEvent{
		ID:        "single-1",
		Type:      policy.EventTypePolicyRemoved,
		PType:     "g",
		OldPolicy: []string{"role:admin", "user:alice"},
		NewPolicy: nil,
		Source:    "node-single",
		Timestamp: time.Now(),
	}

	data, err := MarshalEvent(original)
	require.NoError(t, err)

	decoded, err := UnmarshalEvent(data)
	require.NoError(t, err)
	defer releaseEvent(decoded)

	assert.Equal(t, "single-1", decoded.ID)
	assert.Equal(t, policy.EventTypePolicyRemoved, decoded.Type)
	assert.Equal(t, "g", decoded.PType)
	assert.Equal(t, []string{"role:admin", "user:alice"}, decoded.OldPolicy)
	assert.Nil(t, decoded.NewPolicy)
}

func TestBufferPoolReuse(t *testing.T) {
	event := &policy.ChangeEvent{
		ID:        "pool-test",
		Type:      policy.EventTypePolicyUpdated,
		PType:     "p",
		NewPolicy: []string{"x", "y", "z"},
		Source:    "node-pool",
		Timestamp: time.Now(),
	}

	// 第一次编码
	data1, err := MarshalEvent(event)
	require.NoError(t, err)
	originalCap := cap(data1)
	releaseBuffer(data1)

	// 第二次编码，应该复用池中的 buffer
	data2, err := MarshalEvent(event)
	require.NoError(t, err)
	assert.Equal(t, originalCap, cap(data2), "buffer capacity should be reused from pool")
	releaseBuffer(data2)
}

func TestEventPoolReuse(t *testing.T) {
	// 获取一个 event
	e1 := acquireEvent()
	e1.ID = "reuse-test"
	e1.Type = policy.EventTypePolicyCleared
	e1.NewPolicy = []string{"a", "b"}
	releaseEvent(e1)

	// 获取第二个 event，应该是复用的同一个对象
	e2 := acquireEvent()
	defer releaseEvent(e2)

	assert.Equal(t, "", e2.ID, "reused event should be reset")
	assert.Equal(t, policy.ChangeEventType(""), e2.Type, "reused event type should be reset")
	assert.Nil(t, e2.NewPolicy, "reused event NewPolicy should be nil")
}

func TestUnmarshalInvalidData(t *testing.T) {
	// 过短的数据
	_, err := UnmarshalEvent([]byte{1, 2, 3})
	assert.Error(t, err)

	// 空数据
	_, err = UnmarshalEvent([]byte{})
	assert.Error(t, err)
}

// TestReadLenStringLengthExceedsBuffer 覆盖 readLenString 中 off+n > len(buf) 分支
func TestReadLenStringLengthExceedsBuffer(t *testing.T) {
	// 长度前缀声明 5 字节，但实际只跟 1 字节
	bad := make([]byte, 0)
	bad = binary.BigEndian.AppendUint32(bad, 5)
	bad = append(bad, 'x')
	_, err := UnmarshalEvent(bad)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds buffer")
}

// TestUnmarshalEventFieldErrors 覆盖 UnmarshalEvent 各字段读取失败的错误路径
func TestUnmarshalEventFieldErrors(t *testing.T) {
	// 构建合法前缀的辅助函数
	validPrefix := func() []byte {
		buf := make([]byte, 0)
		buf = appendLenString(buf, "id")
		buf = appendLenString(buf, string(policy.EventTypePolicyAdded))
		buf = appendLenString(buf, "p")
		buf = appendStringSlice(buf, []string{"a"})
		buf = appendStringSlice(buf, []string{"b"})
		buf = appendLenString(buf, "src")
		return buf
	}

	t.Run("BadType", func(t *testing.T) {
		buf := make([]byte, 0)
		buf = appendLenString(buf, "id")
		buf = append(buf, 0, 0, 0, 5) // Type 长度=5，无数据
		_, err := UnmarshalEvent(buf)
		assert.Error(t, err)
	})

	t.Run("BadPType", func(t *testing.T) {
		buf := make([]byte, 0)
		buf = appendLenString(buf, "id")
		buf = appendLenString(buf, string(policy.EventTypePolicyAdded))
		buf = append(buf, 0, 0, 0, 5) // PType 长度=5，无数据
		_, err := UnmarshalEvent(buf)
		assert.Error(t, err)
	})

	t.Run("OldPolicyCountTruncated", func(t *testing.T) {
		buf := make([]byte, 0)
		buf = appendLenString(buf, "id")
		buf = appendLenString(buf, string(policy.EventTypePolicyAdded))
		buf = appendLenString(buf, "p")
		buf = append(buf, 0, 0, 0) // OldPolicy count 前缀只有 3 字节
		_, err := UnmarshalEvent(buf)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unexpected end of buffer")
	})

	t.Run("OldPolicyElementBad", func(t *testing.T) {
		buf := make([]byte, 0)
		buf = appendLenString(buf, "id")
		buf = appendLenString(buf, string(policy.EventTypePolicyAdded))
		buf = appendLenString(buf, "p")
		buf = append(buf, 0, 0, 0, 1) // OldPolicy count=1
		buf = append(buf, 0, 0, 0, 5) // 元素长度=5，无数据
		_, err := UnmarshalEvent(buf)
		assert.Error(t, err)
	})

	t.Run("NewPolicyCountTruncated", func(t *testing.T) {
		buf := make([]byte, 0)
		buf = appendLenString(buf, "id")
		buf = appendLenString(buf, string(policy.EventTypePolicyAdded))
		buf = appendLenString(buf, "p")
		buf = appendStringSlice(buf, []string{"a"})
		buf = append(buf, 0, 0, 0) // NewPolicy count 前缀只有 3 字节
		_, err := UnmarshalEvent(buf)
		assert.Error(t, err)
	})

	t.Run("BadSource", func(t *testing.T) {
		buf := make([]byte, 0)
		buf = appendLenString(buf, "id")
		buf = appendLenString(buf, string(policy.EventTypePolicyAdded))
		buf = appendLenString(buf, "p")
		buf = appendStringSlice(buf, []string{"a"})
		buf = appendStringSlice(buf, []string{"b"})
		buf = append(buf, 0, 0, 0, 5) // Source 长度=5，无数据
		_, err := UnmarshalEvent(buf)
		assert.Error(t, err)
	})

	t.Run("TimestampTruncated", func(t *testing.T) {
		buf := validPrefix()
		// validPrefix 包含完整 Source，追加部分时间戳（不足 8 字节）
		buf = append(buf, 0, 0, 0) // 只有 3 字节时间戳
		_, err := UnmarshalEvent(buf)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "timestamp exceeds buffer")
	})
}

// TestReadStringSliceEmptyAndErrors 直接测试 readStringSlice 的边界
func TestReadStringSliceEmptyAndErrors(t *testing.T) {
	t.Run("EmptySlice", func(t *testing.T) {
		buf := make([]byte, 0)
		buf = binary.BigEndian.AppendUint32(buf, 0) // count=0
		ss, off, err := readStringSlice(buf, 0)
		require.NoError(t, err)
		assert.Nil(t, ss)
		assert.Equal(t, 4, off)
	})

	t.Run("CountExceedsBuffer", func(t *testing.T) {
		// count 前缀不完整
		buf := []byte{0, 0, 0}
		_, _, err := readStringSlice(buf, 0)
		assert.Error(t, err)
	})
}

func BenchmarkCodecMarshal(b *testing.B) {
	event := &policy.ChangeEvent{
		ID:        "bench-id-123456",
		Type:      policy.EventTypePolicyAdded,
		PType:     "p",
		NewPolicy: []string{"alice", "data1", "read"},
		Source:    "node-bench",
		Timestamp: time.Now(),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, _ := MarshalEvent(event)
		releaseBuffer(data)
	}
}

func BenchmarkCodecUnmarshal(b *testing.B) {
	event := &policy.ChangeEvent{
		ID:        "bench-id-123456",
		Type:      policy.EventTypePolicyAdded,
		PType:     "p",
		NewPolicy: []string{"alice", "data1", "read"},
		Source:    "node-bench",
		Timestamp: time.Now(),
	}
	data, _ := MarshalEvent(event)
	defer releaseBuffer(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e, _ := UnmarshalEvent(data)
		releaseEvent(e)
	}
}

// BenchmarkJSONMarshal 对照组：原 JSON 序列化性能
func BenchmarkJSONMarshal(b *testing.B) {
	event := &policy.ChangeEvent{
		ID:        "bench-id-123456",
		Type:      policy.EventTypePolicyAdded,
		PType:     "p",
		NewPolicy: []string{"alice", "data1", "read"},
		Source:    "node-bench",
		Timestamp: time.Now(),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = json.Marshal(event)
	}
}
