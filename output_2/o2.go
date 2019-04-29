package main

import (
	"fmt"
	"strconv"
)

func main() {
	n := 50
	x := 0
	for i := 0; i < n; i++ {
		fmt.Println("    orderer" + strconv.Itoa(i) + ".example.com:")
		fmt.Println("        extends:")
		fmt.Println("            file: docker-compose-base.yml")
		fmt.Println("            service: orderer")
		fmt.Println("        container_name: orderer" + strconv.Itoa(i) + ".example.com")
		fmt.Println("        environment:")
		fmt.Println("            - ORDERER_HOST=orderer" + strconv.Itoa(i) + ".example.com")
		fmt.Println("            - CONFIGTX_ORDERER_ORDERERTYPE=solo")
		fmt.Println("            - RABBITMQ_CONNECT=rabbitmq0:5672")
		fmt.Println("            - ORDERER_ABSOLUTEMAXBYTES=${ORDERER_ABSOLUTEMAXBYTES}")
		fmt.Println("            - ORDERER_PREFERREDMAXBYTES=${ORDERER_PREFERREDMAXBYTES}")
		fmt.Println("        volumes:")
		fmt.Println("            - ../crypto-config/ordererOrganizations/example.com/orderers/orderer0.example.com/msp:/var/hyperledger/msp")
		fmt.Println("            - ../crypto-config/ordererOrganizations/example.com/orderers/orderer0.example.com/tls:/var/hyperledger/tls")
		fmt.Println("            - ../config/:/var/hyperledger/configs")
		fmt.Println("        depends_on:")
		fmt.Println("            - rabbitmq0")
		fmt.Println("        networks:")
		fmt.Println("          behave:")
		fmt.Println("             aliases:")
		fmt.Println("               - ${CORE_PEER_NETWORKID}")
		fmt.Println("        ports:")
		fmt.Println("          - " + strconv.Itoa(7050+x) + ":7050")
		fmt.Println("")
		x += 500
	}
}
