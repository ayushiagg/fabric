package main

import (
	"fmt"
	"strconv"
)

func main() {
	n := 50
	x := 0
	for i := 0; i < n; i++ {
		fmt.Println("  orderer" + strconv.Itoa(i) + ".example.com:")
		fmt.Println("    container_name: orderer" + strconv.Itoa(i) + ".example.com")
		fmt.Println("    image: hyperledger/fabric-orderer")
		fmt.Println("    environment:")
		fmt.Println("      - FABRIC_LOGGING_SPEC=debug")
		fmt.Println("      - ORDERER_HOST=orderer" + strconv.Itoa(i) + ".example.com")
		fmt.Println("      - RABBITMQ_CONNECT=rabbitmq0:5672")
		fmt.Println("      - ORDERER_GENERAL_LISTENADDRESS=0.0.0.0")
		fmt.Println("      - ORDERER_GENERAL_GENESISMETHOD=file")
		fmt.Println("      - ORDERER_GENERAL_GENESISFILE=/var/hyperledger/orderer/orderer.genesis.block")
		fmt.Println("      - ORDERER_GENERAL_LOCALMSPID=OrdererMSP")
		fmt.Println("      - ORDERER_GENERAL_LOCALMSPDIR=/var/hyperledger/orderer/msp")
		fmt.Println("      # enabled TLS")
		fmt.Println("      - ORDERER_GENERAL_TLS_ENABLED=true")
		fmt.Println("      - ORDERER_GENERAL_TLS_PRIVATEKEY=/var/hyperledger/orderer/tls/server.key")
		fmt.Println("      - ORDERER_GENERAL_TLS_CERTIFICATE=/var/hyperledger/orderer/tls/server.crt")
		fmt.Println("      - ORDERER_GENERAL_TLS_ROOTCAS=[/var/hyperledger/orderer/tls/ca.crt, /var/hyperledger/peerOrg1/tls/ca.crt, /var/hyperledger/peerOrg2/tls/ca.crt]")
		fmt.Println("    working_dir: /opt/gopath/src/github.com/hyperledger/fabric")
		fmt.Println("    command: orderer")
		fmt.Println("    volumes:")
		fmt.Println("      - /home/ayushi/go/src/github.com/hyperledger/fabric-samples/first-network/channel-artifacts/genesis.block:/var/hyperledger/orderer/orderer.genesis.block")
		fmt.Println("      - /home/ayushi/go/src/github.com/hyperledger/fabric-samples/first-network/crypto-config/ordererOrganizations/example.com/orderers/orderer" + strconv.Itoa(i) + ".example.com/:/var/hyperledger/orderer")
		fmt.Println("      - orderer" + strconv.Itoa(i) + ".example.com:/var/hyperledger/production/orderer")
		fmt.Println("      - /home/ayushi/go/src/github.com/hyperledger/fabric-samples/bin/consumer:/var/hyperledger/fabric/orderer/elastico")
		fmt.Println("      - /home/ayushi/go/src/github.com/hyperledger/fabric-samples/first-network/crypto-config/peerOrganizations/org1.example.com/peers/peer0.org1.example.com/:/var/hyperledger/peerOrg1")
		fmt.Println("      - /home/ayushi/go/src/github.com/hyperledger/fabric-samples/first-network/crypto-config/peerOrganizations/org2.example.com/peers/peer0.org2.example.com/:/var/hyperledger/peerOrg2")
		fmt.Println("    ports:")
		fmt.Println("      - " + strconv.Itoa(7050+x) + ":7050")
		fmt.Println("")
		x += 500
	}
}
