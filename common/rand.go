package common

import (
	randc "crypto/rand"
	"math/big"
	"math/rand"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

// RandSeq return random & fixed length sequence of string
func RandSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

// RandInt64 random 64 bits integer
func RandInt64() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := randc.Int(randc.Reader, max)
	x := bigx.Int64()
	return x
}

// RandUInt64 random 64 bits unsigned integer
func RandUInt64() uint64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := randc.Int(randc.Reader, max)
	x := bigx.Uint64()
	return x
}
