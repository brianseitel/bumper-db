package bumper

import (
	"encoding/binary"
	"io"
	"math"
	"os"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"
)

type Bumper struct {
	Logger    *zap.Logger
	Directory string
	Filename  string
	Handler   *os.File
	KeyDir    map[string]KeyEntry

	Mutex *sync.Mutex
}

type KeyEntry struct {
	FileID        string
	ValueSize     int32
	ValuePosition int64
	Timestamp     int64
}

func New(dir string) *Bumper {
	logger, _ := zap.NewDevelopment()

	db := &Bumper{
		Logger:    logger,
		Directory: dir,
		KeyDir:    make(map[string]KeyEntry),
		Mutex:     &sync.Mutex{},
	}

	filename, err := db.getActiveFile(db.Directory)
	if err != nil {
		filename = db.createActiveFile()
	}

	db.Filename = filename

	return db
}

func (bumper *Bumper) Set(key string, value any) error {
	timestamp := time.Now().Unix()

	kv := KeyValue{
		Timestamp: timestamp,
		Key:       key,
		Value:     value,
	}

	sz, data := kv.Encode()
	bumper.Logger.Sugar().Infof("sz: %d", sz)

	pos, _ := bumper.Handler.Seek(0, io.SeekEnd)
	n, err := bumper.Handler.Write(data)
	if err != nil {
		panic(err)
	}
	bumper.Handler.Sync()
	bumper.Logger.Sugar().Infof("wrote %d bytes", n)

	ke := KeyEntry{
		Timestamp:     timestamp,
		ValueSize:     int32(sz),
		ValuePosition: pos,
	}

	bumper.KeyDir[key] = ke
	bumper.Handler.Seek(0, io.SeekEnd)
	return nil
}

func (s *Bumper) Get(key string) any {
	kv, ok := s.KeyDir[key]

	if !ok {
		return ""
	}

	s.Logger.Sugar().Infof("seeking to offset: %d", kv.ValuePosition)
	// Reset to start of file
	s.Handler.Seek(kv.ValuePosition, io.SeekStart)

	// Load the data
	bufferSize := HeaderSize + len(key) + int(kv.ValueSize)
	s.Logger.Sugar().Infof("setting buffer size: %d", bufferSize)
	data := make([]byte, bufferSize)
	_, err := s.Handler.Read(data)
	if err != nil {
		panic(err)
	}

	entry := &KeyValue{}
	header := entry.Decode(data)

	return fromBytes(entry.Value, header.ValType)
}

func float64FromBytes(bytes []byte) float64 {
	bits := binary.LittleEndian.Uint64(bytes)
	float := math.Float64frombits(uint64(bits))
	return float
}

func intFromBytes(bytes []byte) int {
	bits := binary.LittleEndian.Uint64(bytes)
	return int(bits)
}

func (s *Bumper) ListKeys() []string {
	var keys []string

	for key := range s.KeyDir {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	return keys
}

const deleteSequence = "[DELETED]\u0022"

func (bumper *Bumper) Delete(key string) error {
	timestamp := time.Now().Unix()

	value := deleteSequence
	kv := KeyValue{
		Timestamp: timestamp,
		Key:       key,
		Value:     []byte(value),
	}

	sz, data := kv.Encode()
	bumper.Logger.Sugar().Infof("sz: %d", sz)

	n, err := bumper.Handler.Write(data)
	if err != nil {
		panic(err)
	}
	bumper.Handler.Sync()
	bumper.Logger.Sugar().Infof("wrote %d bytes", n)

	delete(bumper.KeyDir, key)
	bumper.Handler.Seek(0, io.SeekEnd)
	return nil
}
