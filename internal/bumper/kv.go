package bumper

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const (
	TypeInteger = 0
	TypeString  = 1
	TypeBytes   = 2
	TypeByte    = 3
	TypeFloat   = 4
)

const HeaderSize = 20 // size of header in bytes: 8 + 4 + 4 + 4 = 20

type Header struct {
	Timestamp int64
	KeySize   int32
	ValSize   int32
	ValType   int32
}

func (h Header) Encode() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, h.Timestamp)
	binary.Write(buf, binary.LittleEndian, h.KeySize)
	binary.Write(buf, binary.LittleEndian, h.ValSize)
	binary.Write(buf, binary.LittleEndian, h.ValType)
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

	vt := make([]byte, 4)
	_, err = reader.ReadAt(vt, 16)
	if err != nil {
		panic(err)
	}

	h.Timestamp = int64(binary.LittleEndian.Uint64(ts))
	h.KeySize = int32(binary.LittleEndian.Uint32(ks))
	h.ValSize = int32(binary.LittleEndian.Uint32(vs))
	h.ValType = int32(binary.LittleEndian.Uint32(vt))
}

type KeyValue struct {
	Timestamp int64
	Key       string
	Value     any
	Size      int
}

func (kv KeyValue) Encode() (int, []byte) {
	value, valueType := toBytes(kv.Value)

	header := Header{
		Timestamp: kv.Timestamp,
		KeySize:   int32(len(kv.Key)),
		ValSize:   int32(len(value)),
		ValType:   int32(valueType),
	}

	buf := new(bytes.Buffer)
	buf.Write(header.Encode())
	buf.Write([]byte(kv.Key))
	buf.Write(value)

	return buf.Len(), buf.Bytes()
}

func toBytes(value any) ([]byte, int) {
	switch t := value.(type) {
	case float64:
		buff := new(bytes.Buffer)
		err := binary.Write(buff, binary.LittleEndian, value.(float64))
		if err != nil {
			panic(err)
		}
		return buff.Bytes(), TypeFloat
	case int:
		buff := new(bytes.Buffer)
		err := binary.Write(buff, binary.LittleEndian, uint64(value.(int)))
		if err != nil {
			panic(err)
		}
		return buff.Bytes(), TypeInteger
	case int32:
		buff := new(bytes.Buffer)
		err := binary.Write(buff, binary.LittleEndian, uint64(value.(int32)))
		if err != nil {
			panic(err)
		}
		return buff.Bytes(), TypeInteger
	case int64:
		buff := new(bytes.Buffer)
		err := binary.Write(buff, binary.LittleEndian, uint64(value.(int64)))
		if err != nil {
			panic(err)
		}
		return buff.Bytes(), TypeInteger
	case string:
		return []byte(value.(string)), TypeString
	case byte:
		return []byte{value.(byte)}, TypeByte
	case []byte:
		return value.([]byte), TypeBytes
	default:
		panic(fmt.Sprintf("invalid type: %T", t))
	}

	panic("invalid type")
}

func fromBytes(value any, valueType int32) any {
	switch valueType {
	case TypeFloat:
		switch value.(type) {
		case float64:
			return value.(float64)
		}
		return float64FromBytes(value.([]byte))
	case TypeInteger:
		switch value.(type) {
		case int:
			return value.(int)
		}
		return intFromBytes(value.([]byte))
	case TypeString:
		switch value.(type) {
		case string:
			return value.(string)
		case []byte:
			return string(value.([]byte))
		}
	case TypeByte:
		return value.(byte)
	case TypeBytes:
		return value.([]byte)
	}

	panic("invalid type")
	return nil
}

func (kv *KeyValue) Decode(data []byte) Header {
	reader := bytes.NewReader(data)

	header := Header{}
	header.Decode(data)

	offset := int64(HeaderSize)
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

	val := fromBytes(value, header.ValType)
	kv.Value = val
	kv.Size = len(data)

	return header
}
