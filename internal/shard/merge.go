package shard

type HintEntry struct {
	Timestamp int64
	KeySize   int16
	ValueSize int32
	ValuePos  int64
	Key       string
}

// func (shard *Shard) Merge() error {
// 	start := time.Now()
// 	files, err := os.ReadDir(shard.Directory)
// 	if err != nil {
// 		panic(err)
// 	}

// 	var hints []HintEntry
// 	for _, file := range files {
// 		f, err := os.Open(fmt.Sprintf("%s/%s", shard.Directory, file.Name()))
// 		if err != nil {
// 			panic(err)
// 		}

// 		for {
// 			result, err := getNextKey(f)
// 			if err == nil {
// 				shard.Logger.Sugar().Infof("found key for entry %s", result.Key)
// 				hints = append(hints, HintEntry{
// 					Timestamp: result.TS,
// 					KeySize:   result.KeySize,
// 					ValueSize: result.ValSize,
// 					Key:       result.Key,
// 				})
// 			} else {
// 				if err.Error() == "EOF" {
// 					break
// 				}
// 			}
// 		}
// 	}

// 	num := 1
// 	f, _ := os.Create(fmt.Sprintf("%s/%d.hint", shard.Directory, num))
// 	defer f.Close()
// 	for _, hint := range hints {
// 		pos, _ := f.Seek(0, io.SeekEnd)

// 		buf := new(bytes.Buffer)
// 		binary.Write(buf, binary.LittleEndian, hint.Timestamp) // 64
// 		binary.Write(buf, binary.LittleEndian, hint.KeySize)   // 16
// 		binary.Write(buf, binary.LittleEndian, hint.ValueSize) // 32
// 		binary.Write(buf, binary.LittleEndian, pos)            // 64
// 		buf.Write([]byte(hint.Key))

// 		f.Write(buf.Bytes())
// 	}
// 	f.Sync()

// 	shard.Logger.Sugar().Infof("Finished initializing DB in %s", time.Since(start))
// 	return nil
// }
