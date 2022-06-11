package util

import (
	"bytes"
	"encoding/binary"
)

func Byte2Int(val []byte) int {
	bf := bytes.NewBuffer(val)
	var x int32
	_ = binary.Read(bf, binary.LittleEndian, &x)
	return int(x)
}

func Int2Byte(val int) []byte {
	x := int32(val)
	bf := bytes.NewBuffer([]byte{})
	_ = binary.Write(bf, binary.LittleEndian, x)
	return bf.Bytes()
}

func Byte2Float(val []byte) float32 {
	bf := bytes.NewBuffer(val)
	var x float32
	_ = binary.Read(bf, binary.LittleEndian, &x)
	return x
}

func Float2Byte(val float32) []byte {
	bf := bytes.NewBuffer([]byte{})
	_ = binary.Write(bf, binary.LittleEndian, val)
	return bf.Bytes()
}
