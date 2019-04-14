package main

import (
	"fmt"
	"strconv"
)

func main() {
	n := 66

	for i := 0; i < n; i++ {

		fmt.Println("\torderer" + strconv.Itoa(i) + ".example.com:")
		fmt.Println("\t\textends:")
		fmt.Println("\t\t\tfile: docker-compose-base.yml")
		fmt.Println("\t\t\tservice: orderer")
		fmt.Println("\t\tcontainer_name: orderer" + strconv.Itoa(i) + ".example.com")
		fmt.Println("\t\tenvironment:")
		fmt.Println("\t\t\t- ORDERER_HOST=orderer" + strconv.Itoa(i) + ".example.com")
		fmt.Println("\t\t\t- CONFIGTX_ORDERER_ORDERERTYPE=kafka")
		fmt.Println("\t\t\t- CONFIGTX_ORDERER_KAFKA_BROKERS=[kafka0:9092,kafka1:9092,kafka2:9092,kafka3:9092]")
		fmt.Println("\t\t\t- ORDERER_KAFKA_RETRY_SHORTINTERVAL=1s")
		fmt.Println("\t\t\t- ORDERER_KAFKA_RETRY_SHORTTOTAL=30s")
		fmt.Println("\t\t\t- ORDERER_KAFKA_VERBOSE=true")
		fmt.Println("\t\t\t- ORDERER_GENERAL_GENESISPROFILE=SampleInsecureKafka")
		fmt.Println("\t\t\t- ORDERER_ABSOLUTEMAXBYTES=${ORDERER_ABSOLUTEMAXBYTES}")
		fmt.Println("\t\t\t- ORDERER_PREFERREDMAXBYTES=${ORDERER_PREFERREDMAXBYTES}")
		fmt.Println("\t\tvolumes:")
		fmt.Println("\t\t\t- ../crypto-config/ordererOrganizations/example.com/orderers/orderer0.example.com/msp:/var/hyperledger/msp")
		fmt.Println("\t\t\t- ../crypto-config/ordererOrganizations/example.com/orderers/orderer0.example.com/tls:/var/hyperledger/tls")
		fmt.Println("\t\t\t- ../config/:/var/hyperledger/configs")
		fmt.Println("\t\tdepends_on:")
		fmt.Println("\t\t\t- kafka0")
		fmt.Println("\t\t\t- kafka1")
		fmt.Println("\t\t\t- kafka2")
		fmt.Println("\t\t\t- kafka3")
		fmt.Println("\t\tnetworks:")
		fmt.Println("\t\t  behave:")
		fmt.Println("\t\t\t aliases:")
		fmt.Println("\t\t\t   - ${CORE_PEER_NETWORKID}")
		fmt.Println("\t\tports:")
		fmt.Println("\t\t  - 7050:7050")
		fmt.Println("")
	}

}
