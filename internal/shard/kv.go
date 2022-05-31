package shard

import (
	"bytes"
	"encoding/binary"
)

type Header struct {
	Timestamp int64
	KeySize   int32
	ValSize   int32
}

func (h Header) Encode() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, h.Timestamp)
	binary.Write(buf, binary.LittleEndian, h.KeySize)
	binary.Write(buf, binary.LittleEndian, h.ValSize)

	return buf.Bytes()
}

func (h *Header) Decode(data []byte) {
	reader := bytes.NewReader(data)

	ts := make([]byte, 8)
	_, err := reader.ReadAt(ts, 0)
	if err != nil {
		panic(err)
	}

	ks := make([]byte, 4)
	_, err = reader.ReadAt(ks, 8)
	if err != nil {
		panic(err)
	}

	vs := make([]byte, 4)
	_, err = reader.ReadAt(vs, 12)
	if err != nil {
		panic(err)
	}

	h.Timestamp = int64(binary.LittleEndian.Uint64(ts))
	h.KeySize = int32(binary.LittleEndian.Uint32(ks))
	h.ValSize = int32(binary.LittleEndian.Uint32(vs))
}

type KeyValue struct {
	Timestamp int64
	Key       string
	Value     string
	Size      int
}

func (kv KeyValue) Encode() (int, []byte) {

	header := Header{
		Timestamp: kv.Timestamp,
		KeySize:   int32(len(kv.Key)),
		ValSize:   int32(len(kv.Value)),
	}

	buf := new(bytes.Buffer)
	buf.Write(header.Encode())
	buf.Write([]byte(kv.Key))
	buf.Write([]byte(kv.Value))

	return buf.Len(), buf.Bytes()
}

func (kv *KeyValue) Decode(data []byte) {
	reader := bytes.NewReader(data)

	header := Header{}
	header.Decode(data)

	offset := int64(16)
	key := make([]byte, header.KeySize)
	_, err := reader.ReadAt(key, offset)
	if err != nil {
		panic(err)
	}
	offset += int64(len(key))

	value := make([]byte, header.ValSize)
	_, err = reader.ReadAt(value, offset)
	if err != nil {
		panic(err)
	}

	kv.Timestamp = header.Timestamp
	kv.Key = string(key)
	kv.Value = string(value)
	kv.Size = len(data)
}
