package shard

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

var tempFile *os.File

func TestMain(m *testing.M) {
	setUp()

	exitVal := m.Run()

	tearDown()

	os.Exit(exitVal)
}

func setUp() {
	var err error
	tempFile, err = os.CreateTemp("/tmp", "dbtest")
	if err != nil {
		panic(err)
	}
	if err != nil {
		panic(err)
	}
}

func tearDown() {
	err := os.Remove(tempFile.Name())
	if err != nil {
		panic(err)
	}
}

func TestInitDB(t *testing.T) {
	shard := Shard{
		Logger:    zap.NewNop(),
		Directory: "/tmp",
		Filename:  tempFile.Name(),
	}

	err := shard.InitDB()

	assert.Nil(t, err)
}

func TestSet(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	db := &Shard{
		Logger:    logger,
		Directory: "/tmp",
		Filename:  tempFile.Name(),
		KeyDir:    make(map[string]KeyEntry),
	}

	db.InitDB()
	assert.NotNil(t, db.Handler)

	err := db.Set("name", "jojo")
	assert.Nil(t, err)

	val := db.Get("name")

	assert.Nil(t, err)
	assert.Equal(t, "jojo", val)
}

func TestInvalidKey(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	db := &Shard{
		Logger:    logger,
		Directory: "/tmp",
		Filename:  tempFile.Name(),
		KeyDir:    make(map[string]KeyEntry),
	}

	db.InitDB()
	assert.NotNil(t, db.Handler)
	val := db.Get("name")
	assert.Equal(t, "", val)
}

func TestPersistence(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	db := &Shard{
		Logger:    logger,
		Directory: "/tmp",
		Filename:  tempFile.Name(),
		KeyDir:    make(map[string]KeyEntry),
	}

	db.InitDB()
	assert.NotNil(t, db.Handler)

	err := db.Set("name", "jojo")
	assert.Nil(t, err)
	db.Handler.Close()

	db.InitDB()

	val := db.Get("name")
	assert.Equal(t, "jojo", val)
}

func TestGetActiveFile(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	db := &Shard{
		Logger:    logger,
		Directory: "",
		Filename:  tempFile.Name(),
		KeyDir:    make(map[string]KeyEntry),
	}
	db.Directory = db.getDataDirectory()
	file, err := db.getActiveFile(db.Directory)

	assert.Nil(t, err)
	assert.Equal(t, "1.db", file)
}

func TestCreateActiveFile(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	db := &Shard{
		Logger:    logger,
		Directory: "",
		Filename:  tempFile.Name(),
		KeyDir:    make(map[string]KeyEntry),
	}
	db.Directory = db.getDataDirectory()
	file := db.createActiveFile()

	assert.Equal(t, "1.db", file)

	file = db.createActiveFile()
	assert.Equal(t, "2.db", file)

	file = db.createActiveFile()
	assert.Equal(t, "3.db", file)

	os.Remove(fmt.Sprintf("%s/1.db", db.Directory))
	os.Remove(fmt.Sprintf("%s/2.db", db.Directory))
	os.Remove(fmt.Sprintf("%s/3.db", db.Directory))
}
