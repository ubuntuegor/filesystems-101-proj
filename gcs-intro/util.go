package main

import (
	"crypto/rand"
	"encoding/json"
)

func makeRandBuf(len int) []byte {
	buf := make([]byte, len)
	n, err := rand.Read(buf)
	if err != nil || n != len {
		panic("failed to make a random buffer")
	}
	return buf
}

func saveJson(x any) []byte {
	b, err := json.Marshal(x)
	if err != nil {
		panic("json.Marshal() failed")
	}
	return b
}
