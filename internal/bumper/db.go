package bumper

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

func (bumper *Bumper) InitDB() error {
	bumper.Logger.Sugar().Infof("Initalizing DB")
	start := time.Now()

	if err := bumper.loadRawFile(bumper.Filename); err != nil {
		filename := bumper.createActiveFile()
		bumper.loadRawFile(filename)
	}

	bumper.initKeyDir()
	bumper.Logger.Sugar().Infof("Finished initializing DB in %s", time.Since(start))

	return nil
}

func (bumper *Bumper) getDataDirectory() string {
	wd, _ := os.Getwd()
	wd = wd + "/../../data"
	return wd
}

func (bumper *Bumper) getActiveFile(dir string) (string, error) {
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

func (bumper *Bumper) createActiveFile() string {
	files, err := filepath.Glob(fmt.Sprintf("%s/*.db", bumper.Directory))
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
	os.Create(fmt.Sprintf("%s/%s", bumper.Directory, filename))

	return filename
}

func (bumper *Bumper) initKeyDir() error {
	pos, _ := bumper.Handler.Seek(0, io.SeekEnd)

	pos, _ = bumper.Handler.Seek(0, io.SeekStart)
	bumper.Logger.Sugar().Infof("setting offset to %d", pos)
	bumper.Handler.Seek(pos, io.SeekStart)
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
			bumper.KeyDir[kv.Key] = KeyEntry{
				FileID:        bumper.Filename,
				ValueSize:     header.ValSize,
				ValuePosition: pos,
				Timestamp:     header.Timestamp,
			}
		} else {
			delete(bumper.KeyDir, kv.Key)
		}

		offset := HeaderSize + int64(header.KeySize) + int64(header.ValSize)

		bumper.Logger.Sugar().Infof("last offset: %d", pos)
		pos, _ = bumper.Handler.Seek(offset, io.SeekCurrent)
		bumper.Logger.Sugar().Infof("current offset: %d", pos)
	}

	return nil
}

func (bumper *Bumper) loadRawFile(file string) error {
	f, err := os.OpenFile(fmt.Sprintf("%s/%s", bumper.Directory, file), os.O_RDWR, fs.ModeAppend)
	if err != nil {
		return err
	}

	bumper.Logger.Sugar().Infof("loaded file %s", file)
	bumper.Handler = f
	return nil
}

func (bumper *Bumper) Close() error {
	return bumper.Handler.Close()
}
