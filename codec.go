/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2026-07-10 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-07-10 17:28:09
 * @FilePath: \go-casbin-nats-adapter\codec.go
 * @Description: ChangeEvent 二进制编解码，替代 encoding/json 反射路径
 * 手写 length-prefixed 二进制格式，零反射、零逃逸（配合 buffer 池）
 *
 * 编码格式（所有字符串/切片前缀 4 字节 big-endian 长度）：
 *   [len][ID] [len][Type] [len][PType] [len][OldPolicy...] [len][NewPolicy...] [len][Source] [8B UnixNano Timestamp]
 *
 * Copyright (c) 2026 by kamalyes, All Rights Reserved.
 */

package natsadapter

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/kamalyes/go-casbin/policy"
	"github.com/kamalyes/go-toolbox/pkg/syncx"
)

// 编解码常量
const (
	// 每个字符串字段前缀 4 字节长度，最小事件约为 50 字节
	codecBufferInitialCap = 128
)

// bufferPool 复用序列化 buffer，削减 GC 压力
var bufferPool = syncx.NewPool(func() []byte {
	return make([]byte, 0, codecBufferInitialCap)
})

// eventPool 复用 ChangeEvent 对象，减少订阅端堆分配
var eventPool = syncx.NewPool(func() *policy.ChangeEvent {
	return &policy.ChangeEvent{}
})

// acquireBuffer 从池中获取 buffer，返回的 buffer 长度为 0、容量 >= codecBufferInitialCap
func acquireBuffer() []byte {
	return bufferPool.Get()[:0]
}

// releaseBuffer 归还 buffer 到池
// 截断到原始容量以避免持有过大 buffer
func releaseBuffer(buf []byte) {
	bufferPool.Put(buf)
}

// acquireEvent 从池中获取 ChangeEvent 并重置
func acquireEvent() *policy.ChangeEvent {
	e := eventPool.Get()
	*e = policy.ChangeEvent{}
	return e
}

// releaseEvent 归还 ChangeEvent 到池
// 清空引用的 []string 切片以帮助 GC
func releaseEvent(e *policy.ChangeEvent) {
	e.ID = ""
	e.Type = ""
	e.PType = ""
	e.OldPolicy = nil
	e.NewPolicy = nil
	e.Source = ""
	e.Timestamp = time.Time{}
	eventPool.Put(e)
}

// appendLenString 追加长度前缀字符串
func appendLenString(buf []byte, s string) []byte {
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(s)))
	return append(buf, s...)
}

// appendStringSlice 追加字符串切片（总长度前缀 + 每个元素长度前缀）
func appendStringSlice(buf []byte, ss []string) []byte {
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(ss)))
	for _, s := range ss {
		buf = appendLenString(buf, s)
	}
	return buf
}

// readLenString 从指定偏移读取长度前缀字符串
// 返回字符串和新的偏移量
func readLenString(buf []byte, off int) (string, int, error) {
	if off+4 > len(buf) {
		return "", 0, fmt.Errorf("codec: unexpected end of buffer at offset %d", off)
	}
	n := int(binary.BigEndian.Uint32(buf[off:]))
	off += 4
	if off+n > len(buf) {
		return "", 0, fmt.Errorf("codec: string length %d exceeds buffer at offset %d", n, off)
	}
	return string(buf[off : off+n]), off + n, nil
}

// readStringSlice 从指定偏移读取字符串切片
func readStringSlice(buf []byte, off int) ([]string, int, error) {
	if off+4 > len(buf) {
		return nil, 0, fmt.Errorf("codec: unexpected end of buffer at offset %d", off)
	}
	count := int(binary.BigEndian.Uint32(buf[off:]))
	off += 4
	if count == 0 {
		return nil, off, nil
	}
	ss := make([]string, count)
	for i := 0; i < count; i++ {
		var err error
		ss[i], off, err = readLenString(buf, off)
		if err != nil {
			return nil, 0, err
		}
	}
	return ss, off, nil
}

// MarshalEvent 将 ChangeEvent 编码为二进制格式
// 返回的 []byte 由 bufferPool 管理，调用方用完后需 releaseBuffer
func MarshalEvent(event *policy.ChangeEvent) ([]byte, error) {
	buf := acquireBuffer()

	buf = appendLenString(buf, event.ID)
	buf = appendLenString(buf, string(event.Type))
	buf = appendLenString(buf, event.PType)
	buf = appendStringSlice(buf, event.OldPolicy)
	buf = appendStringSlice(buf, event.NewPolicy)
	buf = appendLenString(buf, event.Source)
	// 零值时间用 0 表示，反序列化时还原为 time.Time{}
	ts := uint64(0)
	if !event.Timestamp.IsZero() {
		ts = uint64(event.Timestamp.UnixNano())
	}
	buf = binary.BigEndian.AppendUint64(buf, ts)

	return buf, nil
}

// UnmarshalEvent 从二进制数据解码 ChangeEvent
// 返回的 *ChangeEvent 由 eventPool 管理，调用方用完后需 releaseEvent
func UnmarshalEvent(data []byte) (*policy.ChangeEvent, error) {
	event := acquireEvent()
	off := 0
	var err error

	event.ID, off, err = readLenString(data, off)
	if err != nil {
		releaseEvent(event)
		return nil, err
	}

	var eventType string
	eventType, off, err = readLenString(data, off)
	if err != nil {
		releaseEvent(event)
		return nil, err
	}
	event.Type = policy.ChangeEventType(eventType)

	event.PType, off, err = readLenString(data, off)
	if err != nil {
		releaseEvent(event)
		return nil, err
	}

	event.OldPolicy, off, err = readStringSlice(data, off)
	if err != nil {
		releaseEvent(event)
		return nil, err
	}

	event.NewPolicy, off, err = readStringSlice(data, off)
	if err != nil {
		releaseEvent(event)
		return nil, err
	}

	event.Source, off, err = readLenString(data, off)
	if err != nil {
		releaseEvent(event)
		return nil, err
	}

	if off+8 > len(data) {
		releaseEvent(event)
		return nil, fmt.Errorf("codec: timestamp exceeds buffer at offset %d", off)
	}
	// 零值时间戳（0）还原为 time.Time{}，保持 IsZero() 语义
	ts := int64(binary.BigEndian.Uint64(data[off:]))
	if ts == 0 {
		event.Timestamp = time.Time{}
	} else {
		event.Timestamp = time.Unix(0, ts)
	}

	return event, nil
}
