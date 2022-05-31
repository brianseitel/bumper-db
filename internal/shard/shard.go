package shard

import (
	"io"
	"os"
	"sort"
	"time"

	"go.uber.org/zap"
)

type Shard struct {
	Logger    *zap.Logger
	Directory string
	Filename  string
	Handler   *os.File
	KeyDir    map[string]KeyEntry
}

type KeyEntry struct {
	FileID        string
	ValueSize     int32
	ValuePosition int64
	Timestamp     int64
}

func New(dir string) *Shard {
	logger, _ := zap.NewDevelopment()

	db := &Shard{
		Logger:    logger,
		Directory: dir,
		KeyDir:    make(map[string]KeyEntry),
	}

	filename, err := db.getActiveFile(db.Directory)
	if err != nil {
		filename = db.createActiveFile()
	}

	db.Filename = filename

	return db
}

func (shard *Shard) Set(key string, value string) error {
	timestamp := time.Now().Unix()

	kv := KeyValue{
		Timestamp: timestamp,
		Key:       key,
		Value:     value,
	}

	sz, data := kv.Encode()
	shard.Logger.Sugar().Infof("sz: %d", sz)

	pos, _ := shard.Handler.Seek(0, io.SeekEnd)
	n, err := shard.Handler.Write(data)
	if err != nil {
		panic(err)
	}
	shard.Handler.Sync()
	shard.Logger.Sugar().Infof("wrote %d bytes", n)

	ke := KeyEntry{
		Timestamp:     timestamp,
		ValueSize:     int32(sz),
		ValuePosition: pos,
	}

	shard.KeyDir[key] = ke
	shard.Handler.Seek(0, io.SeekEnd)
	return nil
}

func (s *Shard) Get(key string) string {
	kv, ok := s.KeyDir[key]

	if !ok {
		return ""
	}

	s.Logger.Sugar().Infof("seeking to offset: %d", kv.ValuePosition)
	// Reset to start of file
	s.Handler.Seek(kv.ValuePosition, io.SeekStart)

	// Load the data
	buffserSize := 16 + len(key) + int(kv.ValueSize)
	s.Logger.Sugar().Infof("setting buffer size: %d", buffserSize)
	data := make([]byte, buffserSize)
	_, err := s.Handler.Read(data)
	if err != nil {
		panic(err)
	}

	entry := &KeyValue{}
	entry.Decode(data)

	return string(entry.Value)
}

func (s *Shard) ListKeys() []string {
	var keys []string

	for key := range s.KeyDir {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	return keys
}
