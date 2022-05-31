package shard

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEncodeKV(t *testing.T) {
	kv := KeyValue{
		Timestamp: time.Now().Unix(),
		Key:       "titties",
		Value:     "your mom is a big fat whore",
	}

	size, data := kv.Encode()

	fmt.Println(size)
	result := KeyValue{}
	result.Decode(data)

	assert.Equal(t, result.Timestamp, kv.Timestamp)
	assert.Equal(t, result.Key, kv.Key)
	assert.Equal(t, result.Value, kv.Value)
	assert.Equal(t, result.Size, size)
}
