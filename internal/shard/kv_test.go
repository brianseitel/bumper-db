package shard

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEncodeKV(t *testing.T) {
	kv := KeyValue{
		Timestamp: time.Now().Unix(),
		Key:       "titties",
		Value:     []byte("your mom is a big fat whore"),
	}

	size, data := kv.Encode()

	result := KeyValue{}
	result.Decode(data)

	assert.Equal(t, result.Timestamp, kv.Timestamp)
	assert.Equal(t, result.Key, kv.Key)
	assert.Equal(t, result.Value, kv.Value)
	assert.Equal(t, result.Size, size)
}

func TestDecodeKV(t *testing.T) {

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, 29.5)

	kv := KeyValue{
		Timestamp: time.Now().Unix(),
		Key:       "age",
		Value:     29.5,
	}

	size, data := kv.Encode()

	result := KeyValue{}
	result.Decode(data)

	assert.Equal(t, kv.Timestamp, result.Timestamp)
	assert.Equal(t, kv.Key, result.Key)
	assert.Equal(t, 29.5, result.Value)
	assert.Equal(t, size, result.Size)
}
