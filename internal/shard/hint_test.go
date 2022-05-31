package shard

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHintHeader(t *testing.T) {
	header := Header{
		Timestamp: time.Now().Unix(),
		KeySize:   13,
		ValSize:   431,
	}

	data := header.Encode()
	result := Header{}
	result.Decode(data)

	assert.Equal(t, header.Timestamp, result.Timestamp)
	assert.Equal(t, header.KeySize, result.KeySize)
	assert.Equal(t, header.ValSize, result.ValSize)
}
