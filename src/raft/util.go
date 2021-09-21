package raft

import (
	"fmt"
	"log"
	"math/rand"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// 0 <= l < r
// return [l, r)
func RandInt(l, r int) int {
	if l < 0 || l >= r {
		panic(fmt.Sprintf("RandInt's args must satisfy 0 <= l < r, l: %v, r: %v\n", l, r))
	}
	return rand.Int() % (r - l) + l
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
