package bumper

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"

	"go.uber.org/zap"
)

const HintSize = 24 // size of fixed-length headers

// TODO: create a Hintfile Loader to track the file, instead of
// doing it in the command file.

type HintLoader struct {
	Logger    *zap.Logger
	File      io.ReadWriter
	Directory string
}

func (hl HintLoader) Load(path string) []Hint {
	var hints []Hint

	// Usually passing in .db file, switch to .hint file
	path = strings.Replace(path, ".db", ".hint", -1)

	f, err := os.OpenFile(fmt.Sprintf("%s/%s", hl.Directory, path), os.O_RDWR, os.ModeAppend)
	if err != nil {
		panic(err)
	}
	pos, _ := f.Seek(0, io.SeekStart)
	for {
		hint := Hint{}
		data := make([]byte, 24)
		_, err := f.ReadAt(data, pos)
		if err != nil {
			break
		}
		hint.Decode(data)

		key := make([]byte, hint.KeySize)
		_, err = f.ReadAt(key, pos+24)
		if err != nil {
			panic(err)
		}
		hint.Key = string(key)

		pos += 24 + int64(hint.KeySize)
		hints = append(hints, hint)
	}

	return hints
}

type Hint struct {
	Timestamp int64
	KeySize   int32
	ValueSize int32
	ValuePos  int64
	Key       string
}

func (h *Hint) Encode() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, h.Timestamp)
	binary.Write(buf, binary.LittleEndian, h.KeySize)
	binary.Write(buf, binary.LittleEndian, h.ValueSize)
	binary.Write(buf, binary.LittleEndian, h.ValuePos)
	buf.WriteString(h.Key)

	fmt.Println("len", buf.Len())
	return buf.Bytes()
}

func (h *Hint) Decode(data []byte) error {
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

	vp := make([]byte, 8)
	_, err = reader.ReadAt(vp, 16)
	if err != nil {
		panic(err)
	}

	h.Timestamp = int64(binary.LittleEndian.Uint64(ts))
	h.KeySize = int32(binary.LittleEndian.Uint32(ks))
	h.ValueSize = int32(binary.LittleEndian.Uint32(vs))
	h.ValuePos = int64(binary.LittleEndian.Uint32(vp))
	return nil
}

func (bumper *Bumper) GenerateHintFiles() error {
	pos, _ := bumper.Handler.Seek(0, io.SeekStart)
	bumper.Logger.Sugar().Infof("setting offset to %d", pos)
	bumper.Handler.Seek(pos, io.SeekStart)

	hints := make(map[string]Hint)
	for {
		bumper.Logger.Sugar().Infof("loading %d bytes", HeaderSize)
		data := make([]byte, HeaderSize)
		_, err := bumper.Handler.ReadAt(data, pos)
		if err != nil {
			break
		}
		header := Header{}
		header.Decode(data)

		data = make([]byte, HeaderSize+header.KeySize+header.ValSize)
		bumper.Handler.ReadAt(data, pos)

		kv := KeyValue{}
		kv.Decode(data)

		bumper.Logger.Sugar().Infof("key %s -> val %s", kv.Key, kv.Value)
		// Only add it to KeyDir if it isn't deleted
		if kv.Value != deleteSequence {
			hints[kv.Key] = Hint{
				Timestamp: header.Timestamp,
				KeySize:   int32(len(kv.Key)),
				ValueSize: header.ValSize,
				ValuePos:  pos,
				Key:       kv.Key,
			}
		} else {
			delete(bumper.KeyDir, kv.Key)
		}

		offset := HeaderSize + int64(header.KeySize) + int64(header.ValSize)

		bumper.Logger.Sugar().Infof("last offset: %d", pos)
		pos, _ = bumper.Handler.Seek(offset, io.SeekCurrent)
		bumper.Logger.Sugar().Infof("current offset: %d", pos)
	}

	hintFile := bumper.createHintFile()
	f, _ := os.OpenFile(fmt.Sprintf("%s/%s", bumper.Directory, hintFile), os.O_RDWR, fs.ModeAppend)
	for _, hint := range hints {
		data := hint.Encode()
		f.Write(data)
	}
	f.Close()

	return nil
}
