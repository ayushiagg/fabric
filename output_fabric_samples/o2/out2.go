package main

import (
	"fmt"
	"strconv"
)

func main() {
	n := 50
	for i := 0; i < n; i++ {
		fmt.Println("  orderer" + strconv.Itoa(i) + ".example.com:")
		fmt.Println("    extends:")
		fmt.Println("      file:   base/docker-compose-base.yaml")
		fmt.Println("      service: orderer" + strconv.Itoa(i) + ".example.com")
		fmt.Println("    container_name: orderer" + strconv.Itoa(i) + ".example.com")
		fmt.Println("    networks:")
		fmt.Println("      - byfn")
		fmt.Println("    depends_on:")
		fmt.Println("      - rabbitmq0")
		fmt.Println("")
	}
}
