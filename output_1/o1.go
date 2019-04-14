package main

import (
	"fmt"
	"strconv"
)

func main() {
	n := 66
	for i := 0; i < n; i++ {

		fmt.Println("- orderer" + strconv.Itoa(i) + ".example.com:7050")
	}
}
