package protocol

import (
	"fmt"
)

func PrintHexdump(data []byte) {
	fmt.Printf("Hexdump of data:\n")
	for i, b := range data {
		if i%16 == 0 {
			fmt.Printf("\n%04x  ", i)
		}
		fmt.Printf("%02x ", b)
	}
	fmt.Println()
}
