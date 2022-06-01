package shard

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func (shard *Shard) InitDB() error {
	shard.Logger.Sugar().Infof("Initalizing DB")
	start := time.Now()

	if err := shard.loadRawFile(shard.Filename); err != nil {
		filename := shard.createActiveFile()
		shard.loadRawFile(filename)
	}

	shard.initKeyDir()
	shard.Logger.Sugar().Infof("Finished initializing DB in %s", time.Since(start))

	return nil
}

func (shard *Shard) getDataDirectory() string {
	wd, _ := os.Getwd()
	wd = wd + "/../../data"
	return wd
}

func (shard *Shard) getActiveFile(dir string) (string, error) {
	files, err := filepath.Glob(fmt.Sprintf("%s/*.db", dir))
	if err != nil {
		panic(err)
	}

	latest := ""
	highest := 0
	for _, file := range files {
		slashIdx := strings.LastIndex(file, "/")
		dbIdx := strings.LastIndex(file, ".db")

		numString := file[slashIdx+1 : dbIdx]

		num, _ := strconv.Atoi(numString)
		if num > highest {
			highest = num
			latest = file[slashIdx+1:]
		}
	}

	if latest == "" {
		return "", errors.New("no database found")
	}

	return latest, nil
}

func (shard *Shard) createActiveFile() string {
	files, err := filepath.Glob(fmt.Sprintf("%s/*.db", shard.Directory))
	if err != nil {
		panic(err)
	}

	highest := 0
	for _, file := range files {
		slashIdx := strings.LastIndex(file, "/")
		dbIdx := strings.LastIndex(file, ".db")

		numString := file[slashIdx+1 : dbIdx]

		num, _ := strconv.Atoi(numString)
		if num > highest {
			highest = num
		}
	}

	if highest == 0 {
		highest = 1
	} else {
		highest++
	}

	filename := fmt.Sprintf("%d.db", highest)
	os.Create(fmt.Sprintf("%s/%s", shard.Directory, filename))

	return filename
}

func (shard *Shard) initKeyDir() error {
	pos, _ := shard.Handler.Seek(0, io.SeekEnd)

	pos, _ = shard.Handler.Seek(0, io.SeekStart)
	shard.Logger.Sugar().Infof("setting offset to %d", pos)
	shard.Handler.Seek(pos, io.SeekStart)
	for {
		data := make([]byte, 16)
		_, err := shard.Handler.ReadAt(data, pos)
		if err != nil {
			break
		}
		header := Header{}
		header.Decode(data)

		data = make([]byte, 16+header.KeySize+header.ValSize)
		shard.Handler.ReadAt(data, pos)

		kv := KeyValue{}
		kv.Decode(data)

		shard.Logger.Sugar().Infof("key %s -> val %s", kv.Key, kv.Value)
		// Only add it to KeyDir if it isn't deleted
		if kv.Value != deleteSequence {
			shard.KeyDir[kv.Key] = KeyEntry{
				FileID:        shard.Filename,
				ValueSize:     header.ValSize,
				ValuePosition: pos,
				Timestamp:     header.Timestamp,
			}
		} else {
			delete(shard.KeyDir, kv.Key)
		}

		offset := 16 + int64(header.KeySize) + int64(header.ValSize)

		shard.Logger.Sugar().Infof("last offset: %d", pos)
		pos, _ = shard.Handler.Seek(offset, io.SeekCurrent)
		shard.Logger.Sugar().Infof("current offset: %d", pos)
	}

	return nil
}

func (shard *Shard) loadRawFile(file string) error {
	f, err := os.OpenFile(fmt.Sprintf("%s/%s", shard.Directory, file), os.O_RDWR, fs.ModeAppend)
	if err != nil {
		return err
	}

	shard.Logger.Sugar().Infof("loaded file %s", file)
	shard.Handler = f
	return nil
}

func (shard *Shard) Close() error {
	return shard.Handler.Close()
}
