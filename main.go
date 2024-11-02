package main

import (
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"os"
	"time"
)

// Todo: Implement delete
// Implement CRC
// Implement Generic data types
// Make a tcp server so we can do all these operations
// Persist the in-memory map so we can save data and stuff

const headerSize = 12

func encodeHeader(timestamp uint32, keySize uint32, valueSize uint32) []byte {
	byteArray := make([]byte, 12)
	binary.LittleEndian.PutUint32(byteArray[0:4], timestamp)
	binary.LittleEndian.PutUint32(byteArray[4:8], keySize)
	binary.LittleEndian.PutUint32(byteArray[8:12], valueSize)
	return byteArray
}

func decodeHeader(data []byte) (uint32, uint32, uint32) {
	timestamp := binary.LittleEndian.Uint32(data[0:4])
	keySize := binary.LittleEndian.Uint32(data[4:8])
	valueSize := binary.LittleEndian.Uint32(data[8:12])
	return timestamp, keySize, valueSize
}

func encodeKV(timestamp uint32, key string, value string) ([]byte, int) {
	header := encodeHeader(timestamp, uint32(len(key)), uint32(len(value)))
	keyBytes := []byte(key)
	valueBytes := []byte(value)
	kvBytes := append(header, keyBytes...)
	kvBytes = append(kvBytes, valueBytes...)
	return kvBytes, len(kvBytes)
}

func decodeKV(data []byte) (uint32, string, string) {
	timestamp, keySize, valueSize := decodeHeader(data[0:headerSize])
	key := data[headerSize : headerSize+keySize]
	value := data[headerSize+keySize : headerSize+keySize+valueSize]
	return timestamp, string(key), string(value)
}

type keyDir struct {
	Timestamp uint32
	Position  uint32
	TotalSize uint32
}

func newKeyDir(timestamp uint32, position uint32, totalSize uint32) keyDir {
	return keyDir{Timestamp: timestamp, Position: position, TotalSize: totalSize}
}

type database struct {
	file           *os.File
	offsetPosition int
	keyMap         map[string]keyDir
}

func doesFileExists(fileName string) bool {
	if _, err := os.Stat(fileName); errors.Is(err, os.ErrNotExist) {
		return false
	}

	return true
}

func open(fileName string) *database {
	if !doesFileExists(fileName) {
		file, _ := os.Create(fileName)
		return &database{file: file, keyMap: make(map[string]keyDir)}
	}

	// Opening the database file
	fileDB, err := os.OpenFile(fileName, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatal(err)
	}

	// open the saved in-memory map
	mapFile := "data_map" + ".gob"
	fileMap, err := os.OpenFile(mapFile, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatal(err)
	}

	defer fileMap.Close()

	var keyMap map[string]keyDir
	dec := gob.NewDecoder(fileMap)
	if err := dec.Decode(&keyMap); err != nil {
		log.Fatalf("failed to decode file: %v", err)
	}
	return &database{file: fileDB, keyMap: keyMap}
}

func (d *database) get(key string) string {
	value, ok := d.keyMap[key]

	if !ok {
		return ""
	}

	d.file.Seek(int64(value.Position), 0)
	dataArr := make([]byte, value.TotalSize)
	d.file.Read(dataArr)

	_, _, valueFromFile := decodeKV(dataArr)
	return valueFromFile
}

func (d *database) set(key, value string) {
	timestamp := time.Now().Unix()
	encodedValues, totalSize := encodeKV(uint32(timestamp), key, value)
	d.keyMap[key] = newKeyDir(uint32(timestamp), uint32(d.offsetPosition), uint32(totalSize))
	d.offsetPosition += totalSize
	_, err := d.file.Write(encodedValues)
	if err != nil {
		fmt.Println(err)
	}
}

func (d *database) delete(key string) error {
	_, ok := d.keyMap[key]

	if !ok {
		return fmt.Errorf("key not found!")
	}

	timestamp := time.Now().Unix()
	byteArr, totalSize := encodeKV(uint32(timestamp), key, "deleted")
	d.keyMap[key] = newKeyDir(uint32(timestamp), uint32(d.offsetPosition), uint32(totalSize))
	d.file.Write(byteArr)
	d.offsetPosition += totalSize

	return nil
}

func (d *database) persistKeyDir() error {
	// filename_ represents the keydir we plan on saving
	saveFileName := "data_map" + ".gob"
	file, err := os.OpenFile(saveFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}

	defer file.Close()

	e := gob.NewEncoder(file)
	if err := e.Encode(d.keyMap); err != nil {
		return err
	}

	if err := file.Sync(); err != nil {
		return err
	}

	return nil
}

func (d *database) printMap() {
	fmt.Println(d.keyMap)
}

func (d *database) close() {
	err := d.file.Sync()
	if err != nil {
		fmt.Println(err.Error())
	}

	d.file.Close()
	d.persistKeyDir()
}

func main() {
	d := open("data.db")
	// d.set("hello", "world")
	// d.set("lebron", "james")
	d.printMap()
	d.close()
}
