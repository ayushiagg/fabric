package elastico

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	random "math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/streadway/amqp"
)

// FinNum - final committee id
var FinNum int64

// D - difficulty level , leading bits of PoW must have D 0's (keep w.R.t to hex)
var D = 16

// C - size of committee
var C = 2

// R - number of bits in random string
var R int64 = 8

// s - where 2^s is the number of committees
var s = 2

// ElasticoStates - states reperesenting the running state of the node
var ElasticoStates = map[string]int{"NONE": 0, "PoW Computed": 1, "Formed Identity": 2, "Formed Committee": 3, "RunAsDirectory": 4, "RunAsDirectory after-TxnReceived": 5, "RunAsDirectory after-TxnMulticast": 6, "Receiving Committee Members": 7, "PBFT_NONE": 8, "PBFT_PRE_PREPARE": 9, "PBFT_PRE_PREPARE_SENT": 10, "PBFT_PREPARE_SENT": 11, "PBFT_PREPARED": 12, "PBFT_COMMITTED": 13, "PBFT_COMMIT_SENT": 14, "Intra Consensus Result Sent to Final": 15, "Merged Consensus Data": 16, "FinalPBFT_NONE": 17, "FinalPBFT_PRE_PREPARE": 18, "FinalPBFT_PRE_PREPARE_SENT": 19, "FinalPBFT_PREPARE_SENT": 20, "FinalPBFT_PREPARED": 21, "FinalPBFT_COMMIT_SENT": 22, "FinalPBFT_COMMITTED": 23, "PBFT Finished-FinalCommittee": 24, "CommitmentSentToFinal": 25, "InteractiveConsistencyStarted": 33, "InteractiveConsistencyAchieved": 26, "FinalBlockSent": 27, "FinalBlockReceived": 28, "BroadcastedR": 29, "ReceivedR": 30, "FinalBlockSentToClient": 31, "LedgerUpdated": 32, "Reset": 33}

// IDENTITY :- structure for Identity of nodes
type IDENTITY struct {
	IP              string
	PK              rsa.PublicKey
	CommitteeID     int64
	PoW             PoWmsg
	EpochRandomness string
	Port            string
}

// PoWmsg - PoWmsg
type PoWmsg struct {
	Hash    string
	SetOfRs []string
	Nonce   int
}

// IdentityAndSign :- for signature and its Identity which can be used for verification
type IdentityAndSign struct {
	Sign        string
	Identityobj IDENTITY
}

// FinalBlockData - final block data
type FinalBlockData struct {
	Sent bool
	Txns []Transaction
}

// PrepareMsgData - prepare msg data
type PrepareMsgData struct {
	Digest   string
	Identity IDENTITY
}

// CommitMsgData - commit msg data
type CommitMsgData struct {
	Digest   string
	Identity IDENTITY
}

// PrePrepareMsg - PrePrepare Message
type PrePrepareMsg struct {
	Message        []Transaction
	PrePrepareData PrePrepareContents
	Sign           string
	Identity       IDENTITY
}

// PrePrepareContents - PrePrepare Contents
type PrePrepareContents struct {
	Type   string
	ViewID int
	Seq    int
	Digest string
}

// FinalCommittedBlock :- final committed block that consists of txns and list of signatures and identities
type FinalCommittedBlock struct {
	TxnList                       []Transaction
	ListSignaturesAndIdentityobjs []IdentityAndSign
}

// Elastico :- structure of elastico node
type Elastico struct {
	/*
		connection - rabbitmq connection
		IP - IP address of a node
		Port - unique number for a process
		key - public key and private key pair for a node
		PoW - dict containing 256 bit hash computed by the node, set of Rs needed for epoch randomness, and a nonce
		cur_directory - list of directory members in view of the node
		Identity - Identity consists of Public key, an IP, PoW, committee id, epoch randomness, Port
		committee_id - integer value to represent the committee to which the node belongs
		committee_list - list of nodes in all committees
		committee_Members - set of committee members in its own committee
		is_directory - whether the node belongs to directory committee or not
		is_final - whether the node belongs to final committee or not
		epoch_randomness - R-bit random string generated at the end of previous epoch
		Ri - R-bit random string
		Commitments - set of H(Ri) received by final committee node members and H(Ri) is sent by the final committee node only
		txn_block - block of txns that the committee will agree on(intra committee consensus block)
		set_of_Rs - set of Ris obtained from the final committee of previous epoch
		newset_of_Rs - In the present epoch, set of Ris obtained from the final committee
		CommitteeConsensusData - a dictionary of committee ids that contains a dictionary of the Txn block and the signatures
		FinalBlockbyFinalCommittee - a dictionary of Txn block and the signatures by the final committee members
		state - state in which a node is running
		MergedBlock - list of txns of different committees after their intra committee consensus
		FinalBlock - agreed list of txns after pbft run by final committee
		RcommitmentSet - set of H(Ri)s received from the final committee after the consistency protocol [previous epoch values]
		NewRcommitmentSet - For the present it contains the set of H(Ri)s received from the final committee after the consistency protocol
		FinalCommitteeMembers - members of the final committee received from the directory committee
		Txn- transactions stored by the directory members
		Response - final block to be received by the client
		Flag- to denote a bad or good node
		Views - stores the ports of processes from which committee member Views have been received
		Primary- boolean to denote the Primary node in the committee for PBFT run
		ViewID - view number of the pbft
		PrePrepareMsgLog - log of pre-prepare msgs received during PBFT
		PrepareMsgLog - log of prepare msgs received during PBFT
		CommitMsgLog - log of commit msgs received during PBFT
		PreparedData - data after prepared state
		CommittedData - data after committed state
		Finalpre_prepareMsgLog - log of pre-prepare msgs received during PBFT run by final committee
		FinalprepareMsgLog - log of prepare msgs received during PBFT run by final committee
		FinalcommitMsgLog - log of commit msgs received during PBFT run by final committee
		FinalpreparedData - data after prepared state in final pbft run
		FinalcommittedData - data after committed state in final pbft run
		Faulty - Flag denotes whether this node is Faulty or not
	*/
	Conn         *amqp.Connection
	IP           string
	Port         string
	Key          *rsa.PrivateKey
	PoW          PoWmsg
	CurDirectory []IDENTITY
	Identity     IDENTITY
	CommitteeID  int64
	// only when this node is the member of directory committee
	CommitteeList map[int64][]IDENTITY
	// only when this node is not the member of directory committee
	CommitteeMembers []IDENTITY
	IsDirectory      bool
	IsFinal          bool
	EpochRandomness  string
	Ri               string
	EpochTxns        []Transaction
	// only when this node is the member of final committee
	Commitments                    map[string]bool
	TxnBlock                       []Transaction
	SetOfRs                        map[string]bool
	NewsetOfRs                     map[string]bool
	CommitteeConsensusData         map[int64]map[string][]string
	CommitteeConsensusDataTxns     map[int64]map[string][]Transaction
	FinalBlockbyFinalCommittee     map[string][]IdentityAndSign
	FinalBlockbyFinalCommitteeTxns map[string][]Transaction
	State                          int
	MergedBlock                    []Transaction
	FinalBlock                     FinalBlockData
	RcommitmentSet                 map[string]bool
	NewRcommitmentSet              map[string]bool
	FinalCommitteeMembers          []IDENTITY
	// only when this is the member of the directory committee
	Txn                   map[int64][]Transaction
	Response              []FinalCommittedBlock
	Flag                  bool
	Views                 map[string]bool
	Primary               bool
	ViewID                int
	Faulty                bool
	PrePrepareMsgLog      map[string]PrePrepareMsg
	PrepareMsgLog         map[int]map[int]map[string][]PrepareMsgData
	CommitMsgLog          map[int]map[int]map[string][]CommitMsgData
	PreparedData          map[int]map[int][]Transaction
	CommittedData         map[int]map[int][]Transaction
	FinalPrePrepareMsgLog map[string]PrePrepareMsg
	FinalPrepareMsgLog    map[int]map[int]map[string][]PrepareMsgData
	FinalcommitMsgLog     map[int]map[int]map[string][]CommitMsgData
	FinalpreparedData     map[int]map[int][]Transaction
	FinalcommittedData    map[int]map[int][]Transaction
	EpochcommitmentSet    map[string]bool
	MsgesSameEpoch        []Transaction
}

// Reset :-
func (e *Elastico) Reset() {
	/*
		Reset some of the elastico class members
	*/
	logger.Info("file:- elastico.go, func:- Reset!!")
	e.GetIP()
	e.GetKey()
	// removed queue delete and port update!
	e.PoW = PoWmsg{}
	e.PoW.Hash = ""
	e.PoW.SetOfRs = make([]string, 0)
	e.PoW.Nonce = 0

	e.CurDirectory = make([]IDENTITY, 0)
	// only when this node is the member of directory committee
	e.CommitteeList = make(map[int64][]IDENTITY)
	// only when this node is not the member of directory committee
	e.CommitteeMembers = make([]IDENTITY, 0)
	e.MsgesSameEpoch = make([]Transaction, 0)
	e.Identity = IDENTITY{}
	e.CommitteeID = -1
	var Ri string
	e.Ri = Ri
	e.EpochTxns = make([]Transaction, 0)
	e.IsDirectory = false
	e.IsFinal = false

	// only when this node is the member of final committee
	e.Commitments = make(map[string]bool)
	e.TxnBlock = make([]Transaction, 0)
	e.SetOfRs = e.NewsetOfRs
	e.NewsetOfRs = make(map[string]bool)
	e.CommitteeConsensusData = make(map[int64]map[string][]string)
	e.CommitteeConsensusDataTxns = make(map[int64]map[string][]Transaction)
	e.FinalBlockbyFinalCommittee = make(map[string][]IdentityAndSign)
	e.FinalBlockbyFinalCommitteeTxns = make(map[string][]Transaction)
	e.State = ElasticoStates["NONE"]
	e.MergedBlock = make([]Transaction, 0)

	e.FinalBlock = FinalBlockData{}
	e.FinalBlock.Sent = false
	e.FinalBlock.Txns = make([]Transaction, 0)

	e.RcommitmentSet = e.NewRcommitmentSet
	e.NewRcommitmentSet = make(map[string]bool)
	e.FinalCommitteeMembers = make([]IDENTITY, 0)

	// only when this is the member of the directory committee
	e.Txn = make(map[int64][]Transaction)
	e.Response = make([]FinalCommittedBlock, 0)
	e.Flag = true
	e.Views = make(map[string]bool)
	e.Primary = false
	e.ViewID = 0
	e.Faulty = false

	e.PrePrepareMsgLog = make(map[string]PrePrepareMsg)
	e.PrepareMsgLog = make(map[int]map[int]map[string][]PrepareMsgData)
	e.CommitMsgLog = make(map[int]map[int]map[string][]CommitMsgData)
	e.PreparedData = make(map[int]map[int][]Transaction)
	e.CommittedData = make(map[int]map[int][]Transaction)
	e.FinalPrePrepareMsgLog = make(map[string]PrePrepareMsg)
	e.FinalPrepareMsgLog = make(map[int]map[int]map[string][]PrepareMsgData)
	e.FinalcommitMsgLog = make(map[int]map[int]map[string][]CommitMsgData)
	e.FinalpreparedData = make(map[int]map[int][]Transaction)
	e.FinalcommittedData = make(map[int]map[int][]Transaction)
	e.EpochcommitmentSet = make(map[string]bool)
}

// Reset2 :-
func (e *Elastico) Reset2() {
	/*
		Reset some of the elastico class members
	*/
	logger.Info("file:- elastico.go, func:- Reset2!!")
	// e.GetIP()
	// e.GetKey()
	// removed queue delete and port update!
	// e.PoW = PoWmsg{}
	// e.PoW.Hash = ""
	// e.PoW.SetOfRs = make([]string, 0)
	// e.PoW.Nonce = 0

	// e.CurDirectory = make([]IDENTITY, 0)
	// only when this node is the member of directory committee
	// e.CommitteeList = make(map[int64][]IDENTITY)
	// only when this node is not the member of directory committee
	// e.CommitteeMembers = make([]IDENTITY, 0)
	e.MsgesSameEpoch = make([]Transaction, 0)
	// e.Identity = IDENTITY{}
	// e.CommitteeID = -1
	var Ri string
	e.Ri = Ri
	e.EpochTxns = make([]Transaction, 0)
	// e.IsDirectory = false
	// e.IsFinal = false

	// only when this node is the member of final committee
	e.Commitments = make(map[string]bool)
	e.TxnBlock = make([]Transaction, 0)
	e.SetOfRs = e.NewsetOfRs
	e.NewsetOfRs = make(map[string]bool)
	e.CommitteeConsensusData = make(map[int64]map[string][]string)
	e.CommitteeConsensusDataTxns = make(map[int64]map[string][]Transaction)
	e.FinalBlockbyFinalCommittee = make(map[string][]IdentityAndSign)
	e.FinalBlockbyFinalCommitteeTxns = make(map[string][]Transaction)
	if e.IsDirectory == true {

		e.State = ElasticoStates["RunAsDirectory"]
	} else {
		e.State = ElasticoStates["Formed Committee"]
	}
	e.MergedBlock = make([]Transaction, 0)

	e.FinalBlock = FinalBlockData{}
	e.FinalBlock.Sent = false
	e.FinalBlock.Txns = make([]Transaction, 0)

	e.RcommitmentSet = e.NewRcommitmentSet
	e.NewRcommitmentSet = make(map[string]bool)
	// e.FinalCommitteeMembers = make([]IDENTITY, 0)

	// only when this is the member of the directory committee
	e.Txn = make(map[int64][]Transaction)
	e.Response = make([]FinalCommittedBlock, 0)
	e.Flag = true
	e.Views = make(map[string]bool)
	e.Primary = false
	e.ViewID = 0
	e.Faulty = false

	e.PrePrepareMsgLog = make(map[string]PrePrepareMsg)
	e.PrepareMsgLog = make(map[int]map[int]map[string][]PrepareMsgData)
	e.CommitMsgLog = make(map[int]map[int]map[string][]CommitMsgData)
	e.PreparedData = make(map[int]map[int][]Transaction)
	e.CommittedData = make(map[int]map[int][]Transaction)
	e.FinalPrePrepareMsgLog = make(map[string]PrePrepareMsg)
	e.FinalPrepareMsgLog = make(map[int]map[int]map[string][]PrepareMsgData)
	e.FinalcommitMsgLog = make(map[int]map[int]map[string][]CommitMsgData)
	e.FinalpreparedData = make(map[int]map[int][]Transaction)
	e.FinalcommittedData = make(map[int]map[int][]Transaction)
	e.EpochcommitmentSet = make(map[string]bool)
}

// GetIP :-
func (e *Elastico) GetIP() {
	/*
		for each node(processor) , get IP addr
	*/
	e.IP = os.Getenv("ORDERER_HOST")

}

// RandomGen :-
func RandomGen(R int64) *big.Int {
	/*
		generate a random integer
	*/
	// n is the base, e is the exponent, creating big.Int variables
	var num, e = big.NewInt(2), big.NewInt(R)
	// taking the exponent n to the power e and nil modulo, and storing the result in n
	num.Exp(num, e, nil)
	// generates the random num in the range[0,n)
	// here Reader is a global, shared instance of a cryptographically secure random number generator.
	randomNum, err := rand.Int(rand.Reader, num)

	FailOnError(err, "random number generation", true)
	return randomNum
}

// GetPort :-
func (e *Elastico) GetPort() {
	/*
		get Port number for the process
	*/
	e.Port = os.Getenv("ORDERER_GENERAL_LISTENPORT")
}

// GetKey :-
func (e *Elastico) GetKey() {
	/*
		for each node, it will set key as public pvt key pair
	*/
	var err error
	// generate the public-pvt key pair
	e.Key, err = rsa.GenerateKey(rand.Reader, 2048)
	FailOnError(err, "key generation", true)
}

// InitER :-
func (e *Elastico) InitER() {
	/*
		initialise R-bit epoch random string
	*/
	randomnum := RandomGen(R)
	// set R-bit binary string to epoch randomness
	e.EpochRandomness = fmt.Sprintf("%0"+strconv.FormatInt(R, 10)+"b\n", randomnum)
}

// ElasticoInit :- initialise of data members
func (e *Elastico) ElasticoInit() {
	logger.Info("file:- elastico.go, func:- ElasticoInit()")
	// create rabbit mq connection
	e.Conn = GetConnection()
	// set IP
	e.GetIP()
	e.GetPort()
	// set RSA
	e.GetKey()
	// Initialize PoW!
	e.PoW = PoWmsg{}
	e.PoW.Hash = ""
	e.PoW.SetOfRs = make([]string, 0)
	e.PoW.Nonce = 0

	e.CurDirectory = make([]IDENTITY, 0)

	e.CommitteeList = make(map[int64][]IDENTITY)

	e.CommitteeMembers = make([]IDENTITY, 0)

	e.CommitteeID = -1
	// for setting EpochRandomness
	e.InitER()

	e.Commitments = make(map[string]bool)

	e.TxnBlock = make([]Transaction, 0)

	e.SetOfRs = make(map[string]bool)

	e.NewsetOfRs = make(map[string]bool)

	e.CommitteeConsensusData = make(map[int64]map[string][]string)

	e.CommitteeConsensusDataTxns = make(map[int64]map[string][]Transaction)

	e.FinalBlockbyFinalCommittee = make(map[string][]IdentityAndSign)

	e.FinalBlockbyFinalCommitteeTxns = make(map[string][]Transaction)

	e.State = ElasticoStates["NONE"]

	e.MergedBlock = make([]Transaction, 0)

	e.FinalBlock = FinalBlockData{}
	e.FinalBlock.Sent = false
	e.FinalBlock.Txns = make([]Transaction, 0)

	e.RcommitmentSet = make(map[string]bool)
	e.NewRcommitmentSet = make(map[string]bool)
	e.FinalCommitteeMembers = make([]IDENTITY, 0)

	e.Txn = make(map[int64][]Transaction)
	e.Response = make([]FinalCommittedBlock, 0)
	e.Flag = true
	e.Views = make(map[string]bool)
	e.Primary = false
	e.ViewID = 0
	e.Faulty = false

	e.PrePrepareMsgLog = make(map[string]PrePrepareMsg)
	e.PrepareMsgLog = make(map[int]map[int]map[string][]PrepareMsgData)
	e.CommitMsgLog = make(map[int]map[int]map[string][]CommitMsgData)
	e.PreparedData = make(map[int]map[int][]Transaction)
	e.CommittedData = make(map[int]map[int][]Transaction)
	e.FinalPrePrepareMsgLog = make(map[string]PrePrepareMsg)
	e.FinalPrepareMsgLog = make(map[int]map[int]map[string][]PrepareMsgData)
	e.FinalcommitMsgLog = make(map[int]map[int]map[string][]CommitMsgData)
	e.FinalpreparedData = make(map[int]map[int][]Transaction)
	e.FinalcommittedData = make(map[int]map[int][]Transaction)
	e.EpochcommitmentSet = make(map[string]bool)
}

// ExecutePoW :-
func (e *Elastico) ExecutePoW() {
	/*
		execute PoW
	*/
	e.ComputePoW()
	// if e.Flag {
	// 	// compute Pow for good node
	// } else {
	// 	// compute Pow for bad node
	// 	// e.computeFakePoW()
	// }
}

// ComputePoW :-
func (e *Elastico) ComputePoW() {
	/*
		returns hash which satisfies the difficulty challenge(D) : PoW["Hash"]
	*/
	// logger.Info("file:- elastico.go, func:- ComputePoW()")
	zeroString := ""
	for i := 0; i < D; i++ {
		zeroString += "0"
	}
	if e.State == ElasticoStates["NONE"] {
		nonce := e.PoW.Nonce
		PK := e.Key.PublicKey // public key
		IP := e.IP + e.Port
		// If it is the first epoch , randomsetR will be an empty set .
		// otherwise randomsetR will be any C/2 + 1 random strings Ri that node receives from the previous epoch
		randomsetR := make([]string, 0)
		if len(e.SetOfRs) > 0 {
			e.EpochRandomness, randomsetR = e.XorR()
		}
		// 	compute the digest
		digest := sha256.New()
		digest.Write([]byte(IP))
		digest.Write(PK.N.Bytes())
		digest.Write([]byte(strconv.Itoa(PK.E)))
		digest.Write([]byte(e.EpochRandomness))
		digest.Write([]byte(strconv.Itoa(nonce)))

		hashVal := fmt.Sprintf("%x", digest.Sum(nil))
		bindigest := ""

		for i := 0; i < len(hashVal); i++ {
			intVal, err := strconv.ParseInt(string(hashVal[i]), 16, 0) // converts hex string to integer
			FailOnError(err, "string to int conversion error", true)
			bindigest += fmt.Sprintf("%04b", intVal) // converts intVal to 4 bit binary value
		}
		if strings.HasPrefix(bindigest, zeroString) {
			//hash starts with leading D 0's
			e.PoW.Hash = hashVal
			e.PoW.SetOfRs = randomsetR
			e.PoW.Nonce = nonce
			// change the state after solving the puzzle
			e.State = ElasticoStates["PoW Computed"]
		} else {
			// try for other nonce
			nonce++
			e.PoW.Nonce = nonce
		}
	}
}

// Sample :-
func Sample(A []string, x int) []string {
	// randomly Sample x values from list of strings A
	random.Seed(time.Now().UnixNano())
	randomize := random.Perm(len(A)) // get the random permutation of indices of A

	sampleslice := make([]string, 0)

	for _, v := range randomize[:x] {
		sampleslice = append(sampleslice, A[v])
	}
	return sampleslice
}

// Xorbinary :-
func Xorbinary(A []string) int64 {
	// returns xor of the binary strings of A
	var xorVal int64
	for i := range A {
		intval, _ := strconv.ParseInt(A[i], 2, 0)
		xorVal = xorVal ^ intval
	}
	return xorVal
}

// XorR :-
func (e *Elastico) XorR() (string, []string) {
	logger.Info("file:- elastico.go, func:- XorR()")
	// find xor of any random C/2 + 1 R-bit strings to set the epoch randomness
	listOfRs := make([]string, 0)
	for R := range e.SetOfRs {
		listOfRs = append(listOfRs, R)
	}
	randomset := Sample(listOfRs, C/2+1) //get random C/2 + 1 strings from list of Rs
	xorVal := Xorbinary(randomset)
	xorString := fmt.Sprintf("%0"+strconv.FormatInt(R, 10)+"b\n", xorVal) //converting xor value to R-bit string
	return xorString, randomset
}

//GetCommitteeid :-
func (e *Elastico) GetCommitteeid() {
	/*
		sets last s-bit of PoW["Hash"] as Identity : CommitteeID
	*/
	// logger.Info("file:- elastico.go, func:- GetCommitteeid()")
	PoW := e.PoW.Hash
	bindigest := ""

	for i := 0; i < len(PoW); i++ {
		intVal, err := strconv.ParseInt(string(PoW[i]), 16, 0) // converts hex string to integer
		FailOnError(err, "string to int conversion error", true)
		bindigest += fmt.Sprintf("%04b", intVal) // converts intVal to 4 bit binary value
	}
	// take last s bits of the binary digest
	Identity := bindigest[len(bindigest)-s:]
	iden, err := strconv.ParseInt(Identity, 2, 0) // converts binary string to integer
	FailOnError(err, "binary to int conversion error", true)
	e.CommitteeID = iden
}

// FormIdentity :-
func (e *Elastico) FormIdentity() {
	/*
		Identity formation for a node
		Identity consists of public key, ip, committee id, PoW, nonce, epoch randomness
	*/
	if e.State == ElasticoStates["PoW Computed"] {

		PK := e.Key.PublicKey

		// set the committee id acc to PoW solution
		e.GetCommitteeid()

		e.Identity = IDENTITY{IP: e.IP, PK: PK, CommitteeID: e.CommitteeID, PoW: e.PoW, EpochRandomness: e.EpochRandomness, Port: e.Port}
		// changed the state after Identity formation
		e.State = ElasticoStates["Formed Identity"]
	}
}

// BroadcastToNetwork - Broadcast data to the whole ntw
func BroadcastToNetwork(exchangeName string, msg map[string]interface{}) {
	// logger.Info("file:- elastico.go, func:- BroadcastToNetwork()")
	conn := GetConnection()
	defer conn.Close() // close the connection

	channel := GetChannel(conn)
	defer channel.Close() // close the channel

	body := marshalData(msg)

	// Send the message to the exchange
	err := channel.Publish(
		exchangeName, // exchange
		"",           // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        body,
		})

	FailOnError(err, "Failed to publish a message", true)

}

//Send :-
func (i *IDENTITY) Send(msg map[string]interface{}) {
	/*
		Send the msg to node based on their Identity
	*/
	// establish a connection with RabbitMQ server
	conn := GetConnection()
	defer conn.Close()

	// create a channel
	channel := GetChannel(conn)
	// close the channel
	defer channel.Close()

	queueName := i.IP
	PublishMsg(channel, queueName, msg) // publish the msg in queue
}

// SendToDirectory :- Send about new nodes to directory committee members
func (e *Elastico) SendToDirectory(epoch string) {

	logger.Info("file:- elastico.go, func:- SendToDirectory()")
	// Add the new processor in particular committee list of directory committee nodes
	for _, nodeID := range e.CurDirectory {

		data := map[string]interface{}{"Identity": e.Identity}

		msg := map[string]interface{}{"data": data, "type": "newNode", "epoch": epoch}

		nodeID.Send(msg)
	}
}

// FormCommittee :-
func (e *Elastico) FormCommittee(exchangeName string, epoch string) {
	/*
		creates directory committee if not yet created otherwise informs all the directory members
	*/
	// logger.Info("file:- elastico.go, func:- FormCommittee()")
	if len(e.CurDirectory) < C {

		e.IsDirectory = true

		data := map[string]interface{}{"Identity": e.Identity}
		msg := map[string]interface{}{"data": data, "type": "directoryMember", "epoch": epoch}

		BroadcastToNetwork(exchangeName, msg)
		// change the state as it is the directory member
		e.State = ElasticoStates["RunAsDirectory"]
	} else {

		e.SendToDirectory(epoch)
		if e.State < ElasticoStates["Receiving Committee Members"] {

			e.State = ElasticoStates["Formed Committee"]
		}
	}
}

// VerifyPoW :-
func (e *Elastico) VerifyPoW(Identityobj IDENTITY) bool {
	/*
		verify the PoW of the node Identityobj
	*/
	zeroString := ""
	for i := 0; i < D; i++ {
		zeroString += "0"
	}
	PoW := Identityobj.PoW
	// fmt.Println(PoW)

	// hash := PoW["Hash"].(string)
	hash := PoW.Hash
	// length of hash in hex

	if len(hash) != 64 {
		logger.Error("POW not verified - len of hash less")
		return false
	}

	bindigest := ""

	for i := 0; i < len(hash); i++ {
		intVal, err := strconv.ParseInt(string(hash[i]), 16, 0) // converts hex string to integer
		FailOnError(err, "string to int conversion error", true)
		bindigest += fmt.Sprintf("%04b", intVal) // converts intVal to 4 bit binary value
	}

	// Valid Hash has D leading '0's (in hex)
	if !strings.HasPrefix(bindigest, zeroString) {
		logger.Error("POW not verified - zero not in prefix")
		return false
	}

	// check Digest for set of Ri strings
	// for Ri in PoW["SetOfRs"]:
	// 	digest = e.Hexdigest(Ri)
	// 	if digest not in e.RcommitmentSet:
	// 		return false

	// reconstruct epoch randomness
	EpochRandomness := Identityobj.EpochRandomness
	listOfRs := PoW.SetOfRs
	// listOfRs := PoW["SetOfRs"].([]interface{})
	SetOfRs := make([]string, len(listOfRs))
	for i := range listOfRs {
		SetOfRs[i] = listOfRs[i] //.(string)
	}

	if len(SetOfRs) > 0 {
		xorVal := Xorbinary(SetOfRs)
		EpochRandomness = fmt.Sprintf("%0"+strconv.FormatInt(R, 10)+"b\n", xorVal)
	}

	// recompute PoW

	// public key
	rsaPublickey := Identityobj.PK
	IP := Identityobj.IP + Identityobj.Port
	// nonce := int(PoW["Nonce"].(float64))
	nonce := PoW.Nonce

	// 	compute the digest
	digest := sha256.New()
	digest.Write([]byte(IP))
	digest.Write(rsaPublickey.N.Bytes())
	digest.Write([]byte(strconv.Itoa(rsaPublickey.E)))
	digest.Write([]byte(EpochRandomness))
	digest.Write([]byte(strconv.Itoa(nonce)))

	hashVal := fmt.Sprintf("%x", digest.Sum(nil))
	bindigest = ""

	for i := 0; i < len(hashVal); i++ {
		intVal, err := strconv.ParseInt(string(hashVal[i]), 16, 0) // converts hex string to integer
		FailOnError(err, "string to int conversion error", true)
		bindigest += fmt.Sprintf("%04b", intVal) // converts intVal to 4 bit binary value
	}
	if strings.HasPrefix(bindigest, zeroString) && hashVal == hash {
		// Found a valid Pow, If this doesn't match with PoW["Hash"] then Doesnt verify!
		return true
	}
	if hashVal != hash {
		logger.Error("hashes mismatch")
	}
	logger.Error("POW not verified - prefix not found")
	return false

}

// RunPBFT :-
func (e *Elastico) RunPBFT(epoch string) {
	/*
		Runs a Pbft instance for the intra-committee consensus
	*/
	if e.State == ElasticoStates["PBFT_NONE"] {
		if e.Primary {
			logger.Info("I ammm primary!!!!")
			prePrepareMsg := e.ConstructPrePrepare(epoch) //construct pre-prepare msg
			// multicasts the pre-prepare msg to replicas
			// ToDo: what if Primary does not Send the pre-prepare to one of the nodes
			e.SendPrePrepare(prePrepareMsg)

			// change the state of Primary to pre-prepared
			e.State = ElasticoStates["PBFT_PRE_PREPARE_SENT"]
			// Primary will log the pre-prepare msg for itself
			prePrepareMsgEncoded, _ := json.Marshal(prePrepareMsg["data"])

			var decodedMsg PrePrepareMsg
			err := json.Unmarshal(prePrepareMsgEncoded, &decodedMsg)
			FailOnError(err, "error in unmarshal pre-prepare", true)
			e.LogPrePrepareMsg(decodedMsg)

		} else {

			// for non-Primary members
			if e.IsPrePrepared() {
				e.State = ElasticoStates["PBFT_PRE_PREPARE"]
			}
		}

	} else if e.State == ElasticoStates["PBFT_PRE_PREPARE"] {

		if e.Primary == false {

			// construct prepare msg
			// ToDo: verify whether the pre-prepare msg comes from various primaries or not
			preparemsgList := e.ConstructPrepare(epoch)
			e.SendPrepare(preparemsgList)
			e.State = ElasticoStates["PBFT_PREPARE_SENT"]
		}

	} else if e.State == ElasticoStates["PBFT_PREPARE_SENT"] || e.State == ElasticoStates["PBFT_PRE_PREPARE_SENT"] {
		// ToDo: if, Primary has not changed its state to "PBFT_PREPARE_SENT"
		if e.IsPrepared() {

			// logging.warning("prepared done by %s" , str(e.Port))
			e.State = ElasticoStates["PBFT_PREPARED"]
		}

	} else if e.State == ElasticoStates["PBFT_PREPARED"] {

		commitMsgList := e.ConstructCommit(epoch)
		e.SendCommit(commitMsgList)
		e.State = ElasticoStates["PBFT_COMMIT_SENT"]

	} else if e.State == ElasticoStates["PBFT_COMMIT_SENT"] {

		if e.IsCommitted() {

			// logging.warning("committed done by %s" , str(e.Port))
			e.State = ElasticoStates["PBFT_COMMITTED"]
		}
	}

}

// IsCommitted :-
func (e *Elastico) IsCommitted() bool {
	/*
		Check if the state is committed or not
	*/
	logger.Info("file:- elastico.go, func:- IsCommitted()")
	// collect committed data
	CommittedData := make(map[int]map[int][]Transaction)
	f := (C - 1) / 3
	// check for received request messages
	for socket := range e.PrePrepareMsgLog {
		prePrepareMsg := e.PrePrepareMsgLog[socket]
		// In current View Id
		if prePrepareMsg.PrePrepareData.ViewID == e.ViewID {
			// request msg of pre-prepare request
			requestMsg := prePrepareMsg.Message
			// digest of the message
			digest := prePrepareMsg.PrePrepareData.Digest
			// get sequence number of this msg
			seqnum := prePrepareMsg.PrePrepareData.Seq

			if _, ok := e.PreparedData[e.ViewID]; ok == true {
				_, okk := e.PreparedData[e.ViewID][seqnum]
				if okk == true {
					if txnHexdigest(e.PreparedData[e.ViewID][seqnum]) == digest {
						// pre-prepared matched and prepared is also true, check for commits
						if _, okkk := e.CommitMsgLog[e.ViewID]; okkk == true {

							if _, okkkk := e.CommitMsgLog[e.ViewID][seqnum]; okkkk == true {

								count := 0
								for replicaID := range e.CommitMsgLog[e.ViewID][seqnum] {

									for _, msg := range e.CommitMsgLog[e.ViewID][seqnum][replicaID] {

										if msg.Digest == digest {

											count++
											break
										}
									}
								}
								// ToDo: condition check
								if count >= 2*f+1 {

									if _, presentView := CommittedData[e.ViewID]; presentView == false {

										CommittedData[e.ViewID] = make(map[int][]Transaction)
									}
									if _, presentSeq := CommittedData[e.ViewID][seqnum]; presentSeq == false {

										CommittedData[e.ViewID][seqnum] = make([]Transaction, 0)
									}

									for _, Txn := range requestMsg {

										CommittedData[e.ViewID][seqnum] = append(CommittedData[e.ViewID][seqnum], Txn)
									}
								}
							} else {

								logger.Error("no seqnum found in commit msg log")
							}
						} else {

							logger.Error("no view id found in commit msg log")
						}
					} else {

						logger.Error("wrong digest in is committed")
					}
				} else {
					logger.Error("seqnum not found in IsCommitted")
				}

			} else {

				logger.Error("view not found in IsCommitted")
			}
		} else {

			logger.Error("wrong view in is committed")
		}
	}

	if len(CommittedData) > 0 {

		e.CommittedData = CommittedData
		logger.Info("committed check done by port - ", e.IP)
		return true
	}
	return false
}

// LogPrePrepareMsg :-
func (e *Elastico) LogPrePrepareMsg(msg PrePrepareMsg) {
	/*
		log the pre-prepare msg
	*/
	if len(msg.Message) > 0 {

		logger.Infof("see the logging of preprepare msg %s", strconv.Itoa(len(msg.Message[0].Txn.Payload)))
	} else {
		logger.Info("errorrrrrr!!!!")
	}
	Identityobj := msg.Identity
	IP := Identityobj.IP
	Port := Identityobj.Port
	// create a socket
	socket := IP + ":" + Port
	e.PrePrepareMsgLog[socket] = msg
	// logger.Info("logging the pre-prepare--", e.PrePrepareMsgLog)
}

// LogFinalPrePrepareMsg :-
func (e *Elastico) LogFinalPrePrepareMsg(msg PrePrepareMsg) {
	/*
		log the pre-prepare msg
	*/
	Identityobj := msg.Identity
	IP := Identityobj.IP
	Port := Identityobj.Port
	// create a socket
	socket := IP + ":" + Port
	e.FinalPrePrepareMsgLog[socket] = msg

}

// IsPrepared :-
func (e *Elastico) IsPrepared() bool {
	/*
		Check if the state is prepared or not
	*/
	// logger.Info("file:- elastico.go, func:- IsPrepared()")
	// collect prepared data
	PreparedData := make(map[int]map[int][]Transaction)
	f := (C - 1) / 3
	// check for received request messages
	for socket := range e.PrePrepareMsgLog {

		// In current View Id
		socketMap := e.PrePrepareMsgLog[socket]
		prePrepareData := socketMap.PrePrepareData
		if prePrepareData.ViewID == e.ViewID {

			// request msg of pre-prepare request
			requestMsg := socketMap.Message
			// digest of the message
			digest := prePrepareData.Digest
			// get sequence number of this msg
			seqnum := prePrepareData.Seq
			// find Prepare msgs for this view and sequence number
			_, ok := e.PrepareMsgLog[e.ViewID]

			if ok == true {
				prepareMsgLogViewID := e.PrepareMsgLog[e.ViewID]
				_, okk := prepareMsgLogViewID[seqnum]
				if okk == true {
					// need to find matching prepare msgs from different replicas atleast C/2 + 1
					count := 0
					prepareMsgLogSeq := prepareMsgLogViewID[seqnum]
					for replicaID := range prepareMsgLogSeq {
						prepareMsgLogReplica := prepareMsgLogSeq[replicaID]
						for _, msg := range prepareMsgLogReplica {
							checkdigest := msg.Digest
							if checkdigest == digest {
								count++
								break
							}
						}
					}
					// condition for Prepared state
					if count >= 2*f {

						if _, ok := PreparedData[e.ViewID]; ok == false {

							PreparedData[e.ViewID] = make(map[int][]Transaction)
						}
						preparedViewID := PreparedData[e.ViewID]
						if _, ok := preparedViewID[seqnum]; ok == false {

							preparedViewID[seqnum] = make([]Transaction, 0)
						}
						for _, Txn := range requestMsg {

							preparedViewID[seqnum] = append(preparedViewID[seqnum], Txn)
						}
						PreparedData[e.ViewID][seqnum] = preparedViewID[seqnum]
					}
				}
			}

		}
	}
	if len(PreparedData) > 0 {
		e.PreparedData = PreparedData
		logger.Info("prepared check done")
		return true
	}

	return false
}

// RunFinalPBFT :-
func (e *Elastico) RunFinalPBFT(epoch string) {
	/*
		Run PBFT by final committee members
	*/
	if e.State == ElasticoStates["FinalPBFT_NONE"] {

		if e.Primary {

			fmt.Println("port of final Primary- ", e.Port)
			// construct pre-prepare msg
			finalPrePreparemsg := e.ConstructFinalPrePrepare(epoch)
			// multicasts the pre-prepare msg to replicas
			e.SendPrePrepare(finalPrePreparemsg)

			// change the state of Primary to pre-prepared
			e.State = ElasticoStates["FinalPBFT_PRE_PREPARE_SENT"]
			// Primary will log the pre-prepare msg for itself

			prePrepareMsgEncoded, _ := json.Marshal(finalPrePreparemsg["data"])

			var decodedMsg PrePrepareMsg
			err := json.Unmarshal(prePrepareMsgEncoded, &decodedMsg)
			FailOnError(err, "error in unmarshal final pre-prepare", true)

			e.LogFinalPrePrepareMsg(decodedMsg)

		} else {

			// for non-Primary members
			if e.IsFinalprePrepared() {
				e.State = ElasticoStates["FinalPBFT_PRE_PREPARE"]
			}
		}

	} else if e.State == ElasticoStates["FinalPBFT_PRE_PREPARE"] {

		if e.Primary == false {

			// construct prepare msg
			FinalpreparemsgList := e.ConstructFinalPrepare(epoch)
			e.SendPrepare(FinalpreparemsgList)
			e.State = ElasticoStates["FinalPBFT_PREPARE_SENT"]
		}
	} else if e.State == ElasticoStates["FinalPBFT_PREPARE_SENT"] || e.State == ElasticoStates["FinalPBFT_PRE_PREPARE_SENT"] {

		// ToDo: Primary has not changed its state to "FinalPBFT_PREPARE_SENT"
		if e.IsFinalPrepared() {

			fmt.Println("final prepared done")
			e.State = ElasticoStates["FinalPBFT_PREPARED"]
		}
	} else if e.State == ElasticoStates["FinalPBFT_PREPARED"] {

		commitMsgList := e.ConstructFinalCommit(epoch)
		e.SendCommit(commitMsgList)
		e.State = ElasticoStates["FinalPBFT_COMMIT_SENT"]

	} else if e.State == ElasticoStates["FinalPBFT_COMMIT_SENT"] {

		if e.IsFinalCommitted() {
			for ViewID := range e.FinalcommittedData {

				for seqnum := range e.FinalcommittedData[ViewID] {

					msgList := e.FinalcommittedData[ViewID][seqnum]

					e.FinalBlock.Txns = e.UnionTxns(e.FinalBlock.Txns, msgList)
				}
			}
			e.State = ElasticoStates["FinalPBFT_COMMITTED"]
		}
	}
}

// IsEqual :-
func (t Transaction) IsEqual(transaction Transaction) bool {
	/*
		compare two objs are equal or not
	*/
	//ToDo:- fix this
	logger.Info("file:- elastico.go, func:- IsEqual()")
	return cmp.Equal(t, transaction)

}

// UnionTxns :-
func (e *Elastico) UnionTxns(actualTxns, receivedTxns []Transaction) []Transaction {
	/*
		union the transactions
	*/
	logger.Info("file:- elastico.go, func:- UnionTxns()")
	for _, transaction := range receivedTxns {

		Flag := true
		for _, Txn := range actualTxns {
			if Txn.IsEqual(transaction) {
				Flag = false
				break
			}
		}
		if Flag {
			actualTxns = append(actualTxns, transaction)
		}
	}
	return actualTxns
}

// IsFinalPrepared :-
func (e *Elastico) IsFinalPrepared() bool {
	/*
		Check if the state is prepared or not
	*/
	//  collect prepared data
	PreparedData := make(map[int]map[int][]Transaction)
	f := (C - 1) / 3
	//  check for received request messages
	for socket := range e.FinalPrePrepareMsgLog {

		//  In current View Id
		socketMap := e.FinalPrePrepareMsgLog[socket]
		prePrepareData := socketMap.PrePrepareData
		if prePrepareData.ViewID == e.ViewID {

			//  request msg of pre-prepare request
			requestMsg := socketMap.Message

			//  digest of the message
			digest := prePrepareData.Digest
			//  get sequence number of this msg
			seqnum := prePrepareData.Seq
			//  find Prepare msgs for this view and sequence number
			if _, presentView := e.FinalPrepareMsgLog[e.ViewID]; presentView == true {
				prepareMsgLogViewID := e.FinalPrepareMsgLog[e.ViewID]
				if _, presentSeq := prepareMsgLogViewID[seqnum]; presentSeq == true {

					//  need to find matching prepare msgs from different replicas atleast C//2 + 1
					count := 0
					prepareMsgLogSeq := prepareMsgLogViewID[seqnum]
					for replicaID := range prepareMsgLogSeq {
						prepareMsgLogReplica := prepareMsgLogSeq[replicaID]
						for _, msg := range prepareMsgLogReplica {
							checkdigest := msg.Digest
							if checkdigest == digest {
								count++
								break

							} else {
								logger.Error("checkdigest doest not match")
							}
						}

					}
					//  condition for Prepared state
					if count >= 2*f {

						if _, ok := PreparedData[e.ViewID]; ok == false {

							PreparedData[e.ViewID] = make(map[int][]Transaction)
						} else {
							logger.Error("view not there in prepared data")
						}
						preparedViewID := PreparedData[e.ViewID]
						if _, ok := preparedViewID[seqnum]; ok == false {

							preparedViewID[seqnum] = make([]Transaction, 0)
						} else {
							logger.Error("seq not there in prepared data")
						}
						for _, Txn := range requestMsg {

							preparedViewID[seqnum] = append(preparedViewID[seqnum], Txn)
						}
						PreparedData[e.ViewID][seqnum] = preparedViewID[seqnum]
					} else {
						logger.Error("count is less than 2f")
					}

				} else {
					logger.Error("no seqnum in final preprepare msg log")
				}

			} else {
				logger.Error("no view in final preprepare msg log")
			}
		} else {
			logger.Error("pre-prepare data view id not matched")
		}
	}
	if len(PreparedData) > 0 {

		e.FinalpreparedData = PreparedData
		return true
	}
	logger.Error("len of prepared data == 0")
	return false
}

// IsFinalCommitted :-
func (e *Elastico) IsFinalCommitted() bool {
	/*
		Check if the state is committed or not
	*/
	logger.Info("file:- elastico.go, func:- IsFinalCommitted()")
	// collect committed data
	CommittedData := make(map[int]map[int][]Transaction)
	f := (C - 1) / 3
	// check for received request messages
	for socket := range e.FinalPrePrepareMsgLog {
		prePrepareMsg := e.FinalPrePrepareMsgLog[socket]
		// In current View Id
		if prePrepareMsg.PrePrepareData.ViewID == e.ViewID {
			// request msg of pre-prepare request
			requestMsg := prePrepareMsg.Message
			// digest of the message
			digest := prePrepareMsg.PrePrepareData.Digest
			// get sequence number of this msg
			seqnum := prePrepareMsg.PrePrepareData.Seq
			if _, presentView := e.FinalpreparedData[e.ViewID]; presentView == true {
				if _, presentSeq := e.FinalpreparedData[e.ViewID][seqnum]; presentSeq == true {
					if txnHexdigest(e.FinalpreparedData[e.ViewID][seqnum]) == digest {
						// pre-prepared matched and prepared is also true, check for commits
						if _, ok := e.FinalcommitMsgLog[e.ViewID]; ok == true {
							if _, okk := e.FinalcommitMsgLog[e.ViewID][seqnum]; okk == true {
								count := 0
								for replicaID := range e.FinalcommitMsgLog[e.ViewID][seqnum] {
									for _, msg := range e.FinalcommitMsgLog[e.ViewID][seqnum][replicaID] {

										if msg.Digest == digest {
											count++
											break
										}
									}
								}
								// ToDo: condition check
								if count >= 2*f+1 {
									if _, presentview := CommittedData[e.ViewID]; presentview == false {

										CommittedData[e.ViewID] = make(map[int][]Transaction)
									}
									if _, presentSeq := CommittedData[e.ViewID][seqnum]; presentSeq == false {

										CommittedData[e.ViewID][seqnum] = make([]Transaction, 0)
									}
									for _, Txn := range requestMsg {

										CommittedData[e.ViewID][seqnum] = append(CommittedData[e.ViewID][seqnum], Txn)
									}
								}

							} else {

								logger.Error("no seqnum found in commit msg log")
							}
						} else {
							logger.Error("no view id found in commit msg log")
						}
					} else {

						logger.Error("wrong digest in is final committed")
					}
				} else {

					logger.Error("seq not found in IsCommitted")
				}
			} else {

				logger.Error("view not found in IsCommitted")
			}
		} else {

			logger.Error("wrong view in is committed")
		}
	}
	if len(CommittedData) > 0 {

		e.FinalcommittedData = CommittedData
		return true
	}
	return false
}

// IsPrePrepared :-
func (e *Elastico) IsPrePrepared() bool {
	/*
		if the node received the pre-prepare msg from the Primary
	*/
	// logger.Info("file:- elastico.go, func:- IsPrePrepared()")
	return len(e.PrePrepareMsgLog) > 0
}

// IsFinalprePrepared :-
func (e *Elastico) IsFinalprePrepared() bool {

	logger.Info("file:- elastico.go, func:- IsFinalprePrepared()")
	return len(e.FinalPrePrepareMsgLog) > 0
}

// Hexdigest :-
func (t Transaction) Hexdigest() string {
	/*
		Digest of a transaction
	*/
	logger.Info("file:- elastico.go, func:- Hexdigest of Message()")
	digest := sha256.New()
	digest.Write(t.Txn.Payload)
	digest.Write(t.Txn.Signature)
	digest.Write(t.Txn.XXX_unrecognized)
	digest.Write([]byte(strconv.FormatInt(int64(t.Txn.XXX_sizecache), 10)))
	// } else {
	// 	digest.Write(t.ConfigMsg.Payload)
	// 	digest.Write(t.ConfigMsg.Signature)
	// 	digest.Write(t.ConfigMsg.XXX_unrecognized)
	// 	digest.Write([]byte(strconv.FormatInt(int64(t.ConfigMsg.XXX_sizecache), 10)))
	// }
	digest.Write([]byte(strconv.FormatUint(t.ConfigSeq, 10))) // convert amount(big.Int) to string

	hashVal := fmt.Sprintf("%x", digest.Sum(nil))
	return hashVal
}

// txnHexdigest - Hex digest of Txn List
func txnHexdigest(TxnList []Transaction) string {
	/*
		return Hexdigest for a list of transactions
	*/
	logger.Info("file:- elastico.go, func:- Hexdigest of Txn list()")
	// ToDo : Sort the Txns based on hash value
	txnDigestList := make([]string, len(TxnList))
	for i := 0; i < len(TxnList); i++ {
		txnDigest := TxnList[i].Hexdigest()
		txnDigestList[i] = txnDigest
	}
	sort.Strings(txnDigestList)
	digest := sha256.New()
	for i := 0; i < len(txnDigestList); i++ {
		digest.Write([]byte(txnDigestList[i]))
	}
	hashVal := fmt.Sprintf("%x", digest.Sum(nil)) // hash of the list of txns
	return hashVal
}

//Sign :- Sign the byte array
func (e *Elastico) Sign(digest []byte) string {
	logger.Info("file:- elastico.go, func:- Sign()")
	signed, err := rsa.SignPKCS1v15(rand.Reader, e.Key, crypto.SHA256, digest) // Sign the digest
	FailOnError(err, "Error in Signing byte array", true)
	signature := base64.StdEncoding.EncodeToString(signed) // encode to base64
	return signature
}

// ConstructPrePrepare :-
func (e *Elastico) ConstructPrePrepare(epoch string) map[string]interface{} {
	/*
		construct pre-prepare msg , done by Primary
	*/
	// logger.Info("file:- elastico.go, func:- ConstructPrePrepare()")
	txnBlockList := e.TxnBlock
	logger.Infof("txn block size of construct prepare %s", strconv.Itoa(len(e.TxnBlock[0].Txn.Payload)))
	// ToDo: make prePrepareContents Ordered Dict for signatures purpose
	prePrepareContents := PrePrepareContents{Type: "pre-prepare", ViewID: e.ViewID, Seq: 1, Digest: txnHexdigest(txnBlockList)}

	prePrepareContentsDigest := e.DigestPrePrepareMsg(prePrepareContents)

	data := map[string]interface{}{"Message": txnBlockList, "PrePrepareData": prePrepareContents, "Sign": e.Sign(prePrepareContentsDigest), "Identity": e.Identity}
	prePrepareMsg := map[string]interface{}{"data": data, "type": "pre-prepare", "epoch": epoch}
	return prePrepareMsg
}

// PrepareContents - Prepare Contents
type PrepareContents struct {
	Type   string
	ViewID int
	Seq    int
	Digest string
}

// PrepareMsg - Prepare Msg
type PrepareMsg struct {
	PrepareData PrepareContents
	Sign        string
	Identity    IDENTITY
}

// ConstructPrepare :-
func (e *Elastico) ConstructPrepare(epoch string) []map[string]interface{} {
	/*
		construct prepare msg in the prepare phase
	*/
	// logger.Info("file:- elastico.go, func:- ConstructPrepare()")
	prepareMsgList := make([]map[string]interface{}, 0)
	//  loop over all pre-prepare msgs
	for socketID := range e.PrePrepareMsgLog {

		msg := e.PrePrepareMsgLog[socketID]
		prePreparedData := msg.PrePrepareData
		seqnum := prePreparedData.Seq
		digest := prePreparedData.Digest

		//  make prepare_contents Ordered Dict for signatures purpose
		prepareContents := PrepareContents{Type: "prepare", ViewID: e.ViewID, Seq: seqnum, Digest: digest}
		PrepareContentsDigest := e.DigestPrepareMsg(prepareContents)
		data := map[string]interface{}{"PrepareData": prepareContents, "Sign": e.Sign(PrepareContentsDigest), "Identity": e.Identity}
		preparemsg := map[string]interface{}{"data": data, "type": "prepare", "epoch": epoch}
		prepareMsgList = append(prepareMsgList, preparemsg)
	}
	return prepareMsgList

}

// ConstructFinalPrepare :-
func (e *Elastico) ConstructFinalPrepare(epoch string) []map[string]interface{} {
	/*
		construct prepare msg in the prepare phase
	*/
	logger.Info("file:- elastico.go, func:- ConstructFinalPrepare()")
	FinalprepareMsgList := make([]map[string]interface{}, 0)
	for socketID := range e.FinalPrePrepareMsgLog {

		msg := e.FinalPrePrepareMsgLog[socketID]
		prePreparedData := msg.PrePrepareData
		seqnum := prePreparedData.Seq
		digest := prePreparedData.Digest
		//  make prepare_contents Ordered Dict for signatures purpose

		prepareContents := PrepareContents{Type: "Finalprepare", ViewID: e.ViewID, Seq: seqnum, Digest: digest}
		PrepareContentsDigest := e.DigestPrepareMsg(prepareContents)

		data := map[string]interface{}{"PrepareData": prepareContents, "Sign": e.Sign(PrepareContentsDigest), "Identity": e.Identity}

		prepareMsg := map[string]interface{}{"data": data, "type": "Finalprepare", "epoch": epoch}
		FinalprepareMsgList = append(FinalprepareMsgList, prepareMsg)
	}
	return FinalprepareMsgList
}

// CommitContents - Commit msg Contents
type CommitContents struct {
	Type   string
	ViewID int
	Seq    int
	Digest string
}

// CommitMsg - Commit Msg
type CommitMsg struct {
	Sign       string
	CommitData CommitContents
	Identity   IDENTITY
}

// ConstructCommit :-
func (e *Elastico) ConstructCommit(epoch string) []map[string]interface{} {
	/*
		Construct commit msgs
	*/
	commitMsges := make([]map[string]interface{}, 0)

	for ViewID := range e.PreparedData {

		for seqnum := range e.PreparedData[ViewID] {

			digest := txnHexdigest(e.PreparedData[ViewID][seqnum])
			// make commit_contents Ordered Dict for signatures purpose
			commitContents := CommitContents{Type: "commit", ViewID: ViewID, Seq: seqnum, Digest: digest}
			commitContentsDigest := e.DigestCommitMsg(commitContents)
			data := map[string]interface{}{"Sign": e.Sign(commitContentsDigest), "CommitData": commitContents, "Identity": e.Identity}
			commitMsg := map[string]interface{}{"data": data, "type": "commit", "epoch": epoch}
			commitMsges = append(commitMsges, commitMsg)

		}
	}

	return commitMsges
}

// ConstructFinalCommit :-
func (e *Elastico) ConstructFinalCommit(epoch string) []map[string]interface{} {
	/*
		Construct commit msgs
	*/
	logger.Info("file:- elastico.go, func:- ConstructFinalCommit()")
	commitMsges := make([]map[string]interface{}, 0)

	for ViewID := range e.FinalpreparedData {

		for seqnum := range e.FinalpreparedData[ViewID] {

			digest := txnHexdigest(e.FinalpreparedData[ViewID][seqnum])
			//  make commit_contents Ordered Dict for signatures purpose
			commitContents := CommitContents{Type: "Finalcommit", ViewID: ViewID, Seq: seqnum, Digest: digest}
			commitContentsDigest := e.DigestCommitMsg(commitContents)

			data := map[string]interface{}{"Sign": e.Sign(commitContentsDigest), "CommitData": commitContents, "Identity": e.Identity}
			commitMsg := map[string]interface{}{"data": data, "type": "Finalcommit", "epoch": epoch}
			commitMsges = append(commitMsges, commitMsg)

		}
	}

	return commitMsges
}

// DigestPrePrepareMsg :-
func (e *Elastico) DigestPrePrepareMsg(msg PrePrepareContents) []byte {
	digest := sha256.New()
	digest.Write([]byte(msg.Type))
	digest.Write([]byte(strconv.Itoa(msg.ViewID)))
	digest.Write([]byte(strconv.Itoa(msg.Seq)))
	digest.Write([]byte(msg.Digest))
	return digest.Sum(nil)
}

// DigestPrepareMsg :-
func (e *Elastico) DigestPrepareMsg(msg PrepareContents) []byte {
	digest := sha256.New()
	digest.Write([]byte(msg.Type))
	digest.Write([]byte(strconv.Itoa(msg.ViewID)))
	digest.Write([]byte(strconv.Itoa(msg.Seq)))
	digest.Write([]byte(msg.Digest))
	return digest.Sum(nil)
}

// DigestCommitMsg :-
func (e *Elastico) DigestCommitMsg(msg CommitContents) []byte {
	digest := sha256.New()
	digest.Write([]byte(msg.Type))
	digest.Write([]byte(strconv.Itoa(msg.ViewID)))
	digest.Write([]byte(strconv.Itoa(msg.Seq)))
	digest.Write([]byte(msg.Digest))
	return digest.Sum(nil)
}

// ConstructFinalPrePrepare :-
func (e *Elastico) ConstructFinalPrePrepare(epoch string) map[string]interface{} {
	/*
		construct pre-prepare msg , done by Primary final
	*/
	txnBlockList := e.MergedBlock
	// ToDo :- make pre_prepare_contents Ordered Dict for signatures purpose
	prePrepareContents := PrePrepareContents{Type: "Finalpre-prepare", ViewID: e.ViewID, Seq: 1, Digest: txnHexdigest(txnBlockList)}

	prePrepareContentsDigest := e.DigestPrePrepareMsg(prePrepareContents)

	data := map[string]interface{}{"Message": txnBlockList, "PrePrepareData": prePrepareContents, "Sign": e.Sign(prePrepareContentsDigest), "Identity": e.Identity}
	prePrepareMsg := map[string]interface{}{"data": data, "type": "Finalpre-prepare", "epoch": epoch}
	return prePrepareMsg

}

// SendPrePrepare :-
func (e *Elastico) SendPrePrepare(prePrepareMsg map[string]interface{}) {
	/*
		Send pre-prepare msgs to all committee members
	*/
	for _, nodeID := range e.CommitteeMembers {

		// dont Send pre-prepare msg to self
		if e.Identity.IsEqual(&nodeID) == false {

			nodeID.Send(prePrepareMsg)
		}
	}
}

// SendCommit :-
func (e *Elastico) SendCommit(commitMsgList []map[string]interface{}) {
	/*
		Send the commit msgs to the committee members
	*/
	for _, commitMsg := range commitMsgList {

		for _, nodeID := range e.CommitteeMembers {

			nodeID.Send(commitMsg)
		}
	}
}

// SendPrepare :-
func (e *Elastico) SendPrepare(prepareMsgList []map[string]interface{}) {
	/*
		Send the prepare msgs to the committee members
	*/
	// logger.Info("file:- elastico.go, func:- SendPrepare()")
	// Send prepare msg list to committee members
	for _, preparemsg := range prepareMsgList {

		for _, nodeID := range e.CommitteeMembers {

			nodeID.Send(preparemsg)
		}
	}
}

// DecodeMsgType :-
type DecodeMsgType struct {
	Type    string
	Data    json.RawMessage
	Epoch   string
	Orderer string
}

// NewEpochMsg   :-
type NewEpochMsg struct {
	Data Transaction
}

// ReceiveTxns :-
func (e *Elastico) ReceiveTxns(epochTxn []Transaction) {
	/*
		directory node will Receive transactions from client
	*/
	var k int64
	numOfCommittees := int64(math.Pow(2, float64(s)))
	var num int64
	num = int64(len(epochTxn)) / numOfCommittees // Transactions per committee
	var iden int64
	if num == 0 {
		logger.Info("num is 0")
		for iden = 0; iden < numOfCommittees; iden++ {
			e.Txn[iden] = epochTxn
		}
	} else {

		// loop in sorted order of committee ids
		for iden = 0; iden < numOfCommittees; iden++ {
			if iden == numOfCommittees-1 {
				// give all the remaining txns to the last committee
				e.Txn[iden] = epochTxn[k:]
			} else {
				e.Txn[iden] = epochTxn[k : k+num]
			}
			k = k + num
		}
	}
}

// IsFinalMember :-
func (e *Elastico) IsFinalMember() bool {
	/*
		tell whether this node is a final committee member or not
	*/
	// logger.Info("file:- elastico.go, func:- IsFinalMember()")
	return e.IsFinal
}

// SendtoFinal :- Each committee member sends the signed value(Txn block after intra committee consensus along with signatures to final committee
func (e *Elastico) SendtoFinal(epoch string) {

	logger.Info("file:- elastico.go, func:- SendtoFinal()")
	for ViewID := range e.CommittedData {
		committedDataViewID := e.CommittedData[ViewID]
		for seqnum := range committedDataViewID {

			msgList := committedDataViewID[seqnum]

			e.TxnBlock = e.UnionTxns(e.TxnBlock, msgList)
		}
	}
	logger.Infof("size of fin committee members %s", strconv.Itoa(len(e.FinalCommitteeMembers)))
	logger.Infof("size of txns in Txn block in Send t final func %s", strconv.Itoa(len(e.TxnBlock)))
	for _, finalID := range e.FinalCommitteeMembers {

		//  here TxnBlock is a set, since sets are unordered hence can't Sign them. So convert set to list for signing
		TxnBlock := e.TxnBlock
		data := map[string]interface{}{"Txnblock": TxnBlock, "Sign": e.SignTxnList(TxnBlock), "Identity": e.Identity}
		msg := map[string]interface{}{"data": data, "type": "intraCommitteeBlock", "epoch": epoch}
		finalID.Send(msg)
	}
	e.State = ElasticoStates["Intra Consensus Result Sent to Final"]
}

// SignTxnList :-
func (e *Elastico) SignTxnList(TxnBlock []Transaction) string {
	// Sign the array of Transactions
	digest := sha256.New()
	for i := 0; i < len(TxnBlock); i++ {
		txnDigest := TxnBlock[i].Hexdigest() // Get the transaction digest
		digest.Write([]byte(txnDigest))
	}
	signed, err := rsa.SignPKCS1v15(rand.Reader, e.Key, crypto.SHA256, digest.Sum(nil)) // Sign the digest of Txn List
	FailOnError(err, "Error in Signing Txn List", true)
	signature := base64.StdEncoding.EncodeToString(signed) // encode to base64
	return signature
}

// CheckCountForConsensusData :-
func (e *Elastico) CheckCountForConsensusData() {
	/*
		check the sufficient count for consensus data
	*/
	Flag := false
	var commID int64
	for commID = 0; commID < int64(math.Pow(2, float64(s))); commID++ {

		if _, ok := e.CommitteeConsensusData[commID]; ok == false {

			Flag = true
			break

		} else {

			for txnBlockDigest := range e.CommitteeConsensusData[commID] {

				if len(e.CommitteeConsensusData[commID][txnBlockDigest]) <= C/2 {
					Flag = true
					logger.Warn("bad committee id for intra committee block")
					break
				}
			}
		}
	}
	if Flag == false {

		// when sufficient number of blocks from each committee are received
		logger.Infof("good going for verify and merge by IP %s", e.IP)
		e.VerifyAndMergeConsensusData()
	}

}

// VerifyAndMergeConsensusData :-
func (e *Elastico) VerifyAndMergeConsensusData() {
	/*
		each final committee member validates that the values received from the committees are signed by
		atleast C/2 + 1 members of the proper committee and takes the ordered set union of all the inputs

	*/
	logger.Info("file:- elastico.go, func:- VerifyAndMergeConsensusData()")
	var committeeid int64
	for committeeid = 0; committeeid < int64(math.Pow(2, float64(s))); committeeid++ {

		if _, presentCommID := e.CommitteeConsensusData[committeeid]; presentCommID == true {

			for txnBlockDigest := range e.CommitteeConsensusData[committeeid] {

				if len(e.CommitteeConsensusData[committeeid][txnBlockDigest]) >= C/2+1 {

					// get the txns from the digest
					TxnBlock := e.CommitteeConsensusDataTxns[committeeid][txnBlockDigest]
					if len(TxnBlock) > 0 {

						e.MergedBlock = e.UnionTxns(e.MergedBlock, TxnBlock)
					}
				}
			}
		}
	}
	if len(e.MergedBlock) > 0 {
		logger.Infof("merged block size greater than 0 for ip %s", e.IP)
		// fmt.Println("final committee port - ", e.Port, "has merged data")
		e.State = ElasticoStates["Merged Consensus Data"]
	}
}

// RunInteractiveConsistency :-
func (e *Elastico) RunInteractiveConsistency(epoch string) {
	/*
		Send the H(Ri) to the final committe members.This is done by a final committee member
	*/
	logger.Info("file:- elastico.go, func:- RunInteractiveConsistency()")
	if e.IsFinalMember() == true {
		for _, nodeID := range e.CommitteeMembers {

			logger.Warnf("sent the Int. Commitments  %s to %s", e.IP, nodeID.IP)
			Commitments := MapToList(e.Commitments)
			data := map[string]interface{}{"Identity": e.Identity, "Commitments": Commitments}
			msg := map[string]interface{}{"data": data, "type": "InteractiveConsistency", "epoch": epoch}
			nodeID.Send(msg)
		}
		e.State = ElasticoStates["InteractiveConsistencyStarted"]
	}
}

//Execute :-
func (e *Elastico) Execute(exchangeName string, epoch string, Txn Transaction) string {
	/*
		executing the functions based on the running state
	*/
	config := EState{}
	// initial state of elastico node
	if e.State == ElasticoStates["NONE"] {
		e.ExecutePoW()
	} else if e.State == ElasticoStates["PoW Computed"] {

		// form Identity, when PoW computed
		e.FormIdentity()
	} else if e.State == ElasticoStates["Formed Identity"] {

		// form committee, when formed Identity
		e.FormCommittee(exchangeName, epoch)
	} else if e.IsDirectory && e.State == ElasticoStates["RunAsDirectory"] {

		logger.Infof("The directory member :- %s ", e.IP)
		if len(e.CurDirectory) >= C {
			logger.Info("updating the el state")
			// Modify EL-State in ElasticoState Queue
			e.UpdateElState(strconv.Itoa(ElasticoStates["RunAsDirectory after-TxnReceived"]), epoch)
			e.EpochTxns = append([]Transaction{Txn}, e.EpochTxns...)
			e.ReceiveTxns(e.EpochTxns)
			// directory member has received the txns for all committees
			e.State = ElasticoStates["RunAsDirectory after-TxnReceived"]
		}
	} else if e.State == ElasticoStates["RunAsDirectory after-TxnReceived"] {
		// check when txns received if committee full
		e.CheckCommitteeFull(epoch)
	} else if e.State == ElasticoStates["Receiving Committee Members"] {

		// when a node is part of some committee
		if e.Flag == false {

			// logging the bad nodes
			// logger.Errorf("member with invalid POW %s with commMembers : %s", e.Identity, e.CommitteeMembers)
		}
		// Now The node should go for Intra committee consensus
		// initial state for the PBFT
		e.State = ElasticoStates["PBFT_NONE"]
		// run PBFT for intra-committee consensus
		e.RunPBFT(epoch)

	} else if e.State == ElasticoStates["PBFT_NONE"] || e.State == ElasticoStates["PBFT_PRE_PREPARE"] || e.State == ElasticoStates["PBFT_PREPARE_SENT"] || e.State == ElasticoStates["PBFT_PREPARED"] || e.State == ElasticoStates["PBFT_COMMIT_SENT"] || e.State == ElasticoStates["PBFT_PRE_PREPARE_SENT"] {

		// run pbft for intra consensus
		e.RunPBFT(epoch)
	} else if e.State == ElasticoStates["PBFT_COMMITTED"] {

		// Send pbft consensus blocks to final committee members
		logger.Infof("pbft finished by members %s", e.IP)
		e.SendtoFinal(epoch)

	} else if e.IsFinalMember() && e.State == ElasticoStates["Intra Consensus Result Sent to Final"] {

		// final committee node will collect blocks and merge them
		e.CheckCountForConsensusData()

	} else if e.IsFinalMember() && e.State == ElasticoStates["Merged Consensus Data"] {

		// final committee member runs final pbft
		e.State = ElasticoStates["FinalPBFT_NONE"]
		fmt.Println("start pbft by final member with port--", e.IP)
		e.RunFinalPBFT(epoch)

	} else if e.State == ElasticoStates["FinalPBFT_NONE"] || e.State == ElasticoStates["FinalPBFT_PRE_PREPARE"] || e.State == ElasticoStates["FinalPBFT_PREPARE_SENT"] || e.State == ElasticoStates["FinalPBFT_PREPARED"] || e.State == ElasticoStates["FinalPBFT_COMMIT_SENT"] || e.State == ElasticoStates["FinalPBFT_PRE_PREPARE_SENT"] {

		e.RunFinalPBFT(epoch)

	} else if e.IsFinalMember() && e.State == ElasticoStates["FinalPBFT_COMMITTED"] {

		// Send the commitment to other final committee members
		e.SendCommitment(epoch)
		logger.Infof("pbft finished by final committee by %s", e.IP)

	} else if e.IsFinalMember() && e.State == ElasticoStates["CommitmentSentToFinal"] {

		// broadcast final Txn block to ntw
		if len(e.Commitments) >= C/2+1 {
			logger.Infof("Commitments received sucess by %s", e.IP)
			e.RunInteractiveConsistency(epoch)
		}
	} else if e.IsFinalMember() && e.State == ElasticoStates["InteractiveConsistencyStarted"] {
		if len(e.EpochcommitmentSet) >= C/2+1 {
			e.State = ElasticoStates["InteractiveConsistencyAchieved"]
		} else {
			logger.Warn("Int. Consistency short : ", len(e.EpochcommitmentSet), " port :", e.IP)
		}
	} else if e.IsFinalMember() && e.State == ElasticoStates["InteractiveConsistencyAchieved"] {

		// broadcast final Txn block to ntw
		logger.Info("Consistency received sucess")
		e.BroadcastFinalTxn(epoch, exchangeName)
	} else if e.State == ElasticoStates["FinalBlockReceived"] {

		e.CheckCountForFinalData()

	} else if e.IsFinalMember() && e.State == ElasticoStates["FinalBlockSentToClient"] {

		// broadcast Ri is done when received commitment has atleast C/2  + 1 signatures
		if len(e.NewRcommitmentSet) >= C/2+1 {
			logger.Info("broadacst R by port--", e.Port)
			e.BroadcastR(epoch, exchangeName)
		} else {
			logger.Info("insufficient Rs")
		}
	} else if e.State == ElasticoStates["BroadcastedR"] {
		if len(e.NewsetOfRs) >= C/2+1 {
			logger.Info("received the set of Rs")
			e.State = ElasticoStates["ReceivedR"]
		} else {
			logger.Info("Insuffice Set of Rs")
		}
	} else if e.State == ElasticoStates["ReceivedR"] {

		e.State = ElasticoStates["Reset"]
		// Now, the node can be Reset
		return "Reset"
	}
	if len(e.MsgesSameEpoch) > 0 {
		if len(e.CurDirectory) >= C {
			sameEpochMsg := map[string]interface{}{"Type": "sameEpochMsg", "Epoch": epoch, "Data": e.MsgesSameEpoch}
			for _, node := range e.CurDirectory {
				node.Send(sameEpochMsg)
			}
			// emptying the transactions
			e.MsgesSameEpoch = make([]Transaction, 0)
		}
	}
	config.State = strconv.Itoa(e.State)
	SetState(config, "/conf.json")
	return ""
}

// DigestCommitments :-
func (e *Elastico) DigestCommitments(receivedCommitments []string) []byte {
	digest := sha256.New()
	for _, commitment := range receivedCommitments {
		digest.Write([]byte(commitment))
	}
	return digest.Sum(nil)
}

// MapToList :-
func MapToList(m map[string]bool) []string {
	logger.Info("file:- elastico.go, func:- MapToList()")
	commitmentList := make([]string, len(m))
	i := 0
	for commitment := range m {
		commitmentList[i] = commitment
		i = i + 1
	}
	sort.Strings(commitmentList)
	// logger.Info("commitmentList - ", commitmentList)
	return commitmentList
}

// BroadcastFinalTxn :- final committee members will broadcast S(commitmentSet), along with final set of X(txn_block) to everyone in the network
func (e *Elastico) BroadcastFinalTxn(epoch string, exchangeName string) bool {
	/*
		final committee members will broadcast S(commitmentSet), along with final set of
		X(txn_block) to everyone in the network
	*/
	// ToDo :- implement the consistency protocol
	// boolVal, S := consistencyProtocol(epoch)
	// if boolVal == false {
	// 	logger.Info("Consistency false")
	// 	return false
	// }
	commitmentList := MapToList(e.EpochcommitmentSet)
	commitmentDigest := e.DigestCommitments(commitmentList)
	data := map[string]interface{}{"CommitSet": commitmentList, "Signature": e.Sign(commitmentDigest), "Identity": e.Identity, "FinalBlock": e.FinalBlock.Txns, "FinalBlockSign": e.SignTxnList(e.FinalBlock.Txns)}
	// logger.Warn("finalblock-", e.FinalBlock.Txns)
	// final Block sent to ntw
	e.FinalBlock.Sent = true
	// A final node which is already in received state should not change its state
	if e.State != ElasticoStates["FinalBlockReceived"] {

		e.State = ElasticoStates["FinalBlockSent"]
	}
	msg := map[string]interface{}{"data": data, "type": "FinalBlock", "epoch": epoch}
	BroadcastToNetwork(exchangeName, msg)
	return true
}

// GenerateRandomstrings :-
func (e *Elastico) GenerateRandomstrings() {
	/*
		Generate R-bit random strings
	*/
	if e.IsFinalMember() {
		Ri := RandomGen(R)
		e.Ri = fmt.Sprintf("%0"+strconv.FormatInt(R, 10)+"b\n", Ri)
	}
}

// GetCommitment :-
func (e *Elastico) GetCommitment() string {
	/*
		generate commitment for random string Ri. This is done by a final committee member
	*/
	if e.Ri == "" {
		e.GenerateRandomstrings()
	}
	commitment := sha256.New()
	commitment.Write([]byte(e.Ri))
	hashVal := fmt.Sprintf("%x", commitment.Sum(nil))
	logger.Infof("commitment Ri-- %s of port %s", e.Ri, e.Port)
	logger.Infof("Commitments H(Ri)-- %s", hashVal)
	return hashVal
}

// SendCommitment :-
func (e *Elastico) SendCommitment(epoch string) {
	/*
		Send the H(Ri) to the final committe members.This is done by a final committee member
	*/
	logger.Infof("file:- elastico.go, func:- SendCommitment() by %s", e.IP)
	if e.IsFinalMember() == true {

		HashRi := e.GetCommitment()
		for _, nodeID := range e.CommitteeMembers {

			// logger.Warn("sent the commitment by ", e.Port, " to ", nodeID.Port)
			data := map[string]interface{}{"Identity": e.Identity, "HashRi": HashRi}
			msg := map[string]interface{}{"data": data, "type": "hash", "epoch": epoch}
			nodeID.Send(msg)
		}
		e.State = ElasticoStates["CommitmentSentToFinal"]
	}
}

// GetResponseSize :-
func (e *Elastico) GetResponseSize() int {
	return len(e.Response)
}

// GetResponse :-
func (e *Elastico) GetResponse() []FinalCommittedBlock {
	return e.Response
}

// CheckCountForFinalData :-
func (e *Elastico) CheckCountForFinalData() {
	/*
		check the sufficient counts for final data
	*/
	//  collect final blocks sent by final committee and add the blocks to the Response

	for txnBlockDigest := range e.FinalBlockbyFinalCommittee {

		if len(e.FinalBlockbyFinalCommittee[txnBlockDigest]) >= C/2+1 {

			TxnList := e.FinalBlockbyFinalCommitteeTxns[txnBlockDigest]
			//  create the final committed block that contatins the txnlist and set of signatures and identities to that Txn list
			finalCommittedBlock := FinalCommittedBlock{TxnList, e.FinalBlockbyFinalCommittee[txnBlockDigest]}
			logger.Info("Response received by final committee")
			//  add the block to the Response
			e.Response = append(e.Response, finalCommittedBlock)

		} else {

			// logger.Error("less block signs : ", len(e.FinalBlockbyFinalCommittee[txnBlockDigest]))
		}
	}

	if len(e.Response) > 0 {

		logger.Warnf("final block sent the block to client by %s", e.IP)
		e.State = ElasticoStates["FinalBlockSentToClient"]
	}
}

// BroadcastR :- broadcast Ri to all the network, final member will do this
func (e *Elastico) BroadcastR(epoch string, exchangeName string) {
	logger.Info("file:- elastico.go, func:- BroadcastR()")
	if e.IsFinalMember() {
		// logger.Info("Broadcast Ri , -", e.Ri, " by ", e.Port)
		data := map[string]interface{}{"Ri": e.Ri, "Identity": e.Identity}

		msg := map[string]interface{}{"data": data, "type": "RandomStringBroadcast", "epoch": epoch}

		e.State = ElasticoStates["BroadcastedR"]

		BroadcastToNetwork(exchangeName, msg)

	} else {
		logger.Error("non final member broadcasting R")
	}
}

// Consume :-
func (e *Elastico) Consume(channel *amqp.Channel, queueName string, Txn Transaction, epoch string) {
	/*
		consume the msgs for this node
	*/
	logger.Info("file:- elastico.go, func:- Consume()")
	// count the number of messages that are in the queue
	Queue, err := channel.QueueInspect(queueName)
	// FailOnError(err, "error in inspect", false)

	var decodedmsg DecodeMsgType
	if err == nil {
		// consume all the messages one by one
		for ; Queue.Messages > 0; Queue.Messages-- {

			// get the message from the queue
			msg, ok, err := channel.Get(Queue.Name, true)
			FailOnError(err, "error in get of queue", true)
			if ok {
				err := json.Unmarshal(msg.Body, &decodedmsg)
				FailOnError(err, "error in unmarshall", true)

				if decodedmsg.Epoch == epoch {
					// consume the msg by taking the action in Receive
					e.Receive(decodedmsg, epoch)
				} else {
					// if e.Sta
				}

				//else if decodedmsg.Epoch > epoch {
				// 	requeueMsgs = append(requeueMsgs, msg.Body)
				// 	logger.Warn("Need to requeue msgs! type - ", decodedmsg.Type, " epoch - ", decodedmsg.Epoch, " present epoch : ", epoch)
				// } else {
				// 	logger.Warn("Discarding Msgs type - ", decodedmsg.Type, " epoch - ", decodedmsg.Epoch, " present epoch : ", epoch)
				// }
			}
		}

	}
}

// IdentityInit :- initialise of Identity members
func (i *IDENTITY) IdentityInit() {
	// i.PoW = make(map[string]interface{})
	logger.Info("file:- elastico.go, func:- IdentityInit()")
	i.PoW = PoWmsg{}
}

// IsEqual :-
func (i *IDENTITY) IsEqual(Identityobj *IDENTITY) bool {
	/*
		checking two objects of Identity class are equal or not

	*/
	// comparing uncomparable type []interface {} ie. set Of Rs

	// ToDo: compare set of Rs and fix this

	// listOfRsInI := i.PoW["SetOfRs"].([]interface{})
	// setOfRsInI := make([]string, len(listOfRsInI))
	// for i := range listOfRsInI {
	// 	setOfRsInI[i] = listOfRsInI[i].(string)
	// }

	// listOfRsIniobj := Identityobj.PoW["SetOfRs"].([]interface{})
	// setOfRsIniobj := make([]string, len(listOfRsIniobj))
	// for i := range listOfRsIniobj {
	// 	setOfRsIniobj[i] = listOfRsIniobj[i].(string)
	// }
	// logger.Info("file:- elastico.go, func:- IsEqual of identity()")
	if i.PK.N.Cmp(Identityobj.PK.N) != 0 {
		return false
	}
	return i.IP == Identityobj.IP && i.PK.E == Identityobj.PK.E && i.CommitteeID == Identityobj.CommitteeID && i.PoW.Hash == Identityobj.PoW.Hash && i.PoW.Nonce == Identityobj.PoW.Nonce && i.EpochRandomness == Identityobj.EpochRandomness && i.Port == Identityobj.Port
}

// Dmsg :- structure for directory msg
type Dmsg struct {
	Identity IDENTITY
}

// ReceiveDirectoryMember :-
func (e *Elastico) ReceiveDirectoryMember(msg DecodeMsgType) {
	logger.Infof("file:- elastico.go, func:- ReceiveDirectoryMember() by %s", e.IP)
	var decodeMsg Dmsg

	err := json.Unmarshal(msg.Data, &decodeMsg)
	FailOnError(err, "fail to decode directory member msg", true)

	Identityobj := decodeMsg.Identity
	// verify the PoW of the sender
	if e.VerifyPoW(Identityobj) {
		if len(e.CurDirectory) < C {
			// check whether Identityobj is already present or not
			Flag := true
			for _, obj := range e.CurDirectory {
				if Identityobj.IsEqual(&obj) {
					Flag = false
					break
				}
			}
			if Flag {
				// append the object if not already present
				e.CurDirectory = append(e.CurDirectory, Identityobj)
			}
		}
	} else {
		logger.Error("PoW not valid of an incoming directory member")
	}

}

// NewNodeMsg - Msg for new Node
type NewNodeMsg struct {
	Identity IDENTITY
}

// ReceiveNewNode :-
func (e *Elastico) ReceiveNewNode(msg DecodeMsgType, epoch string) {
	var decodeMsg NewNodeMsg

	err := json.Unmarshal(msg.Data, &decodeMsg)
	FailOnError(err, "decode error in new node msg", true)
	// new node is added to the corresponding committee list if committee list has less than C members
	Identityobj := decodeMsg.Identity
	// verify the PoW
	if e.VerifyPoW(Identityobj) {
		_, ok := e.CommitteeList[Identityobj.CommitteeID]
		if ok == false {

			// Add the Identity in committee
			e.CommitteeList[Identityobj.CommitteeID] = make([]IDENTITY, 0)
			e.CommitteeList[Identityobj.CommitteeID] = append(e.CommitteeList[Identityobj.CommitteeID], Identityobj)

		} else if len(e.CommitteeList[Identityobj.CommitteeID]) < C {
			// Add the Identity in committee
			Flag := true
			for _, obj := range e.CommitteeList[Identityobj.CommitteeID] {
				if Identityobj.IsEqual(&obj) {
					Flag = false
					break
				}
			}
			if Flag {
				e.CommitteeList[Identityobj.CommitteeID] = append(e.CommitteeList[Identityobj.CommitteeID], Identityobj)
				if len(e.CommitteeList[Identityobj.CommitteeID]) == C {
					// check that if all committees are full
					e.CheckCommitteeFull(epoch)
				}
			}
		}

	} else {
		logger.Error("PoW not valid in adding new node")
	}
}

// CheckCommitteeFull :-
func (e *Elastico) CheckCommitteeFull(epoch string) {
	/*
		directory member checks whether the committees are full or not
	*/

	commList := e.CommitteeList
	Flag := 0
	numOfCommittees := int64(math.Pow(2, float64(s)))
	// iterating over all committee ids
	for iden := int64(0); iden < numOfCommittees; iden++ {

		_, ok := commList[iden]
		if ok == false || len(commList[iden]) < C {

			logger.Warnf("committees not full  - bad miss id : %s", strconv.FormatInt(iden, 10))
			Flag = 1
			break
		}
	}
	if Flag == 0 {

		logger.Info("committees full  - good")
		if e.State == ElasticoStates["RunAsDirectory after-TxnReceived"] {

			// notify the final members
			e.NotifyFinalCommittee(epoch)
			// multicast the txns and committee members to the nodes
			MulticastCommittee(commList, e.Identity, e.Txn, epoch)
			// change the state after multicast
			e.State = ElasticoStates["RunAsDirectory after-TxnMulticast"]
		}
	}
}

// UpdateElState :-
func (e *Elastico) UpdateElState(state, epoch string) {
	connection := GetConnection()
	defer connection.Close()
	channel := GetChannel(connection)
	defer channel.Close()
	elasticoStateQueueName := "elasticoState"
	var (
		ElasticoMsg amqp.Delivery
		ok          bool
		errorMsg    error
	)
	for ElasticoMsg, ok, errorMsg = channel.Get(elasticoStateQueueName, true); ok == false || errorMsg != nil; {
		ElasticoMsg, ok, errorMsg = channel.Get(elasticoStateQueueName, true)
	}
	var decodeMsg ElState
	json.Unmarshal(ElasticoMsg.Body, &decodeMsg)
	if decodeMsg.Epoch != epoch {
		logger.Info("different epochs error")
	}
	ElMsg := map[string]interface{}{"State": state, "Epoch": epoch}
	PublishMsg(channel, elasticoStateQueueName, ElMsg)
}

// MulticastCommittee :- each node getting Views of its committee members from directory members
func MulticastCommittee(commList map[int64][]IDENTITY, Identityobj IDENTITY, txns map[int64][]Transaction, epoch string) {

	// get the final committee members with the fixed committee id
	FinalCommitteeMembers := commList[FinNum]
	for CommitteeID, commMembers := range commList {

		// find the Primary Identity, Take the first Identity
		// ToDo: fix this, many nodes can be Primary
		primaryID := commMembers[0]
		for _, memberID := range commMembers {

			// Send the committee members , final committee members
			data := map[string]interface{}{"CommitteeMembers": commMembers, "FinalCommitteeMembers": FinalCommitteeMembers, "Identity": Identityobj}

			// give txns only to the Primary node
			if memberID.IsEqual(&primaryID) {
				data["Txns"] = txns[CommitteeID]
			} else {
				data["Txns"] = make([]Transaction, 0)
			}
			fmt.Println("epoch : ", epoch)
			// construct the msg
			msg := map[string]interface{}{"data": data, "type": "committee members Views", "epoch": epoch}
			// Send the committee member Views to nodes
			memberID.Send(msg)
		}
	}
}

// NotifyFinalCommittee :-
func (e *Elastico) NotifyFinalCommittee(epoch string) {
	/*
		notify the members of the final committee that they are the final committee members
	*/
	finalCommList := e.CommitteeList[FinNum]
	fmt.Println("len of final comm--", len(finalCommList))
	for _, finalMember := range finalCommList {
		data := map[string]interface{}{"Identity": e.Identity}
		// construct the msg
		msg := map[string]interface{}{"data": data, "type": "notify final member", "epoch": epoch}
		finalMember.Send(msg)
	}
}

// CommitmentMsg - Commitment Message
type CommitmentMsg struct {
	Identity IDENTITY
	HashRi   string
}

// ReceiveHash :-
func (e *Elastico) ReceiveHash(msg DecodeMsgType) {

	// receiving H(Ri) by final committe members
	var decodeMsg CommitmentMsg
	err := json.Unmarshal(msg.Data, &decodeMsg)

	FailOnError(err, "fail to decode hash msg", true)

	Identityobj := decodeMsg.Identity
	HashRi := decodeMsg.HashRi
	if e.VerifyPoW(Identityobj) {
		e.Commitments[HashRi] = true
		logger.Info("commitment received-of port", e.Port, e.Commitments)
	} else {
		logger.Error("PoW not verified in receiving Commitments")
	}
}

// BroadcastRmsg :- structure for Broadcast R msg
type BroadcastRmsg struct {
	Ri       string
	Identity IDENTITY
}

// Hexdigest :-
func (e *Elastico) Hexdigest(data string) string {
	/*
		Digest of data
	*/
	logger.Info("file:- elastico.go, func:- Hexdigest of Elastico()")
	digest := sha256.New()
	digest.Write([]byte(data))

	hashVal := fmt.Sprintf("%x", digest.Sum(nil)) // convert to Hexdigest
	return hashVal
}

// ReceiveRandomStringBroadcast :-
func (e *Elastico) ReceiveRandomStringBroadcast(msg DecodeMsgType) {

	var decodeMsg BroadcastRmsg

	err := json.Unmarshal(msg.Data, &decodeMsg)

	FailOnError(err, "fail to decode random string msg", true)

	Identityobj := decodeMsg.Identity
	Ri := decodeMsg.Ri

	if e.VerifyPoW(Identityobj) {
		HashRi := e.Hexdigest(Ri)

		if _, ok := e.NewRcommitmentSet[HashRi]; ok {

			e.NewsetOfRs[Ri] = true

			if len(e.NewsetOfRs) >= C/2+1 {
				logger.Info("received the set of Rs")
				e.State = ElasticoStates["ReceivedR"]
			} else {
				logger.Info("insufficient set of Rs")
			}
		} else {
			logger.Warn("Ri's Commitment not found in CommitmentSet Ri -- ", Ri)
			logger.Warn("Commitments present ", e.NewRcommitmentSet)
		}
	} else {
		logger.Error("POW invalid")
	}
}

// IntraBlockMsg - intra block msg
type IntraBlockMsg struct {
	Txnblock []Transaction
	Sign     string
	Identity IDENTITY
}

// VerifySignTxnList :-
func (e *Elastico) VerifySignTxnList(TxnBlockSignature string, TxnBlock []Transaction, PublicKey *rsa.PublicKey) bool {
	signed, err := base64.StdEncoding.DecodeString(TxnBlockSignature) // Decode the base64 encoded signature
	FailOnError(err, "Decode error of signature", true)
	// Sign the array of Transactions
	digest := sha256.New()
	for i := 0; i < len(TxnBlock); i++ {
		txnDigest := TxnBlock[i].Hexdigest() // Get the transaction digest
		digest.Write([]byte(txnDigest))
	}
	err = rsa.VerifyPKCS1v15(PublicKey, crypto.SHA256, digest.Sum(nil), signed) // verify the Sign of digest of Txn List
	if err != nil {
		return false
	}
	return true
}

// ReceiveIntraCommitteeBlock :-
func (e *Elastico) ReceiveIntraCommitteeBlock(msg DecodeMsgType) {

	// final committee member receives the final set of txns along with the signature from the node
	var decodeMsg IntraBlockMsg
	err := json.Unmarshal(msg.Data, &decodeMsg)
	FailOnError(err, "error in unmarshal intra committee block", true)

	Identityobj := decodeMsg.Identity

	if e.VerifyPoW(Identityobj) {
		signature := decodeMsg.Sign
		TxnBlock := decodeMsg.Txnblock
		logger.Infof("received txn block by final comm size %s", strconv.Itoa(len(TxnBlock)))
		// verify the signatures
		PK := Identityobj.PK
		if e.VerifySignTxnList(signature, TxnBlock, &PK) {
			if _, ok := e.CommitteeConsensusData[Identityobj.CommitteeID]; ok == false {

				e.CommitteeConsensusData[Identityobj.CommitteeID] = make(map[string][]string)
				e.CommitteeConsensusDataTxns[Identityobj.CommitteeID] = make(map[string][]Transaction)
			}
			TxnBlockDigest := txnHexdigest(TxnBlock)
			if _, okk := e.CommitteeConsensusData[Identityobj.CommitteeID][TxnBlockDigest]; okk == false {
				e.CommitteeConsensusData[Identityobj.CommitteeID][TxnBlockDigest] = make([]string, 0)
				// store the txns for this digest
				e.CommitteeConsensusDataTxns[Identityobj.CommitteeID][TxnBlockDigest] = TxnBlock
			}

			// add signatures for the Txn block
			e.CommitteeConsensusData[Identityobj.CommitteeID][TxnBlockDigest] = append(e.CommitteeConsensusData[Identityobj.CommitteeID][TxnBlockDigest], signature)

		} else {
			logger.Error("signature invalid for intra committee block")
		}
	} else {
		logger.Error("pow invalid for intra committee block")
	}

}

//IntraConsistencyMsg :-
type IntraConsistencyMsg struct {
	Identity    IDENTITY
	Commitments []string
}

// Intersection :-
func Intersection(set1 map[string]bool, set2 []string) map[string]bool {
	// This function takes Intersection of two maps
	// intersectSet := make(map[string]bool)
	logger.Info("file:- elastico.go, func:- Intersection()")
	for _, s1 := range set2 {
		// _, ok := set1[s1]
		// if ok == true {
		set1[s1] = true
		// }
	}

	return set1
}

// ReceiveConsistency :-
func (e *Elastico) ReceiveConsistency(msg DecodeMsgType) {
	// Receive consistency msgs
	var decodeMsg IntraConsistencyMsg
	err := json.Unmarshal(msg.Data, &decodeMsg)
	FailOnError(err, "error in unmarshal intra committee block", true)

	Identityobj := decodeMsg.Identity

	if e.VerifyPoW(Identityobj) {
		Commitments := decodeMsg.Commitments
		if len(e.EpochcommitmentSet) == 0 {
			for _, commitment := range Commitments {
				e.EpochcommitmentSet[commitment] = true
			}
		} else {
			e.EpochcommitmentSet = Intersection(e.EpochcommitmentSet, Commitments)
		}
		// logger.Info("received Int. Consistency Commitments! ", len(e.EpochcommitmentSet), " port :", e.Port, " Commitments : ", Commitments)
	} else {
		logger.Error("pow invalid for intra committee block")
	}
}

// Receive :-
func (e *Elastico) Receive(msg DecodeMsgType, epoch string) {
	/*
		method to recieve messages for a node as per the type of a msg
	*/
	// new node is added in directory committee if not yet formed

	if msg.Type == "directoryMember" {
		e.ReceiveDirectoryMember(msg)

	} else if msg.Type == "newNode" && e.IsDirectory {
		e.ReceiveNewNode(msg, epoch)

	} else if msg.Type == "committee members Views" && e.IsDirectory == false {
		e.ReceiveViews(msg)

	} else if msg.Type == "hash" && e.IsFinalMember() {
		e.ReceiveHash(msg)

	} else if msg.Type == "RandomStringBroadcast" {
		e.ReceiveRandomStringBroadcast(msg)

	} else if msg.Type == "pre-prepare" || msg.Type == "prepare" || msg.Type == "commit" {

		e.PbftProcessMessage(msg)
	} else if msg.Type == "intraCommitteeBlock" && e.IsFinalMember() {

		e.ReceiveIntraCommitteeBlock(msg)
	} else if msg.Type == "InteractiveConsistency" && e.IsFinalMember() {

		e.ReceiveConsistency(msg)

	} else if msg.Type == "notify final member" {
		logger.Infof("notifying final member %s", e.Port)
		var decodeMsg NotifyFinalMsg
		err := json.Unmarshal(msg.Data, &decodeMsg)
		FailOnError(err, "error in decoding final member msg", true)
		Identityobj := decodeMsg.Identity
		if e.VerifyPoW(Identityobj) && e.CommitteeID == FinNum {
			e.IsFinal = true
		}
	} else if msg.Type == "Finalpre-prepare" || msg.Type == "Finalprepare" || msg.Type == "Finalcommit" {
		e.FinalpbftProcessMessage(msg)
	} else if msg.Type == "FinalBlock" {

		e.ReceiveFinalTxnBlock(msg)

	} else if msg.Type == "Reset-all" {
		var decodeMsg ResetMsg
		err := json.Unmarshal(msg.Data, &decodeMsg)
		FailOnError(err, "fail to decode Reset msg", true)
		if e.VerifyPoW(decodeMsg.Identity) {
			// Reset the elastico node
			e.Reset()
		}

	} else if msg.Type == "NewMsgSameEpoch" {
		e.HandleNewMsg(msg)
	} else if msg.Type == "sameEpochMsg" {
		var decodeMsg EpochMsg
		err := json.Unmarshal(msg.Data, &decodeMsg)
		FailOnError(err, "fail to decode same epoch msg", true)
		for _, transaction := range decodeMsg.Data {

			Flag := true
			for _, Txn := range e.EpochTxns {
				if Txn.IsEqual(transaction) {
					Flag = false
					break
				}
			}
			if Flag {
				e.EpochTxns = append(e.EpochTxns, transaction)
			}
		}
	}
}

// EpochMsg :-
type EpochMsg struct {
	Data []Transaction
}

// HandleNewMsg :-
func (e *Elastico) HandleNewMsg(msg DecodeMsgType) {
	var sameEpochMessage Transaction
	err := json.Unmarshal(msg.Data, &sameEpochMessage)
	FailOnError(err, "fail to decode same epoch msg", true)
	// store all msgs here
	e.MsgesSameEpoch = append(e.MsgesSameEpoch, sameEpochMessage)
}

// FinalBlockMsg - final block msg
type FinalBlockMsg struct {
	CommitSet      []string
	Signature      string
	Identity       IDENTITY
	FinalBlock     []Transaction
	FinalBlockSign string
}

// IsEqual :-
func (is *IdentityAndSign) IsEqual(data IdentityAndSign) bool {
	/*
		compare two objects
	*/
	logger.Info("file:- elastico.go, func:- IsEqual() of iden and Sign")
	return is.Sign == data.Sign && is.Identityobj.IsEqual(&data.Identityobj)
}

// ReceiveFinalTxnBlock :-
func (e *Elastico) ReceiveFinalTxnBlock(msg DecodeMsgType) {
	var decodeMsg FinalBlockMsg
	err := json.Unmarshal(msg.Data, &decodeMsg)
	FailOnError(err, "fail in decoding the final block msg", true)

	Identityobj := decodeMsg.Identity
	// verify the PoW of the sender
	if e.VerifyPoW(Identityobj) {

		Sign := decodeMsg.Signature
		receivedCommitments := decodeMsg.CommitSet
		finalTxnBlock := decodeMsg.FinalBlock

		finalTxnBlockSignature := decodeMsg.FinalBlockSign

		// verify the signatures
		receivedCommitmentDigest := e.DigestCommitments(receivedCommitments)
		PK := Identityobj.PK
		if e.VerifySign(Sign, receivedCommitmentDigest, &PK) && e.VerifySignTxnList(finalTxnBlockSignature, finalTxnBlock, &PK) {

			// list init for final Txn block
			finaltxnBlockDigest := txnHexdigest(finalTxnBlock)
			if _, ok := e.FinalBlockbyFinalCommittee[finaltxnBlockDigest]; ok == false {
				e.FinalBlockbyFinalCommittee[finaltxnBlockDigest] = make([]IdentityAndSign, 0)
				e.FinalBlockbyFinalCommitteeTxns[finaltxnBlockDigest] = finalTxnBlock
			}

			// creating the object that contains the Identity and signature of the final member
			identityAndSign := IdentityAndSign{finalTxnBlockSignature, Identityobj}

			// check whether this combination of Identity and Sign already exists or not
			Flag := true
			for _, idSignObj := range e.FinalBlockbyFinalCommittee[finaltxnBlockDigest] {

				if idSignObj.IsEqual(identityAndSign) {
					// it exists
					Flag = false
					break
				}
			}
			if Flag {
				// appending the Identity and Sign of final member
				e.FinalBlockbyFinalCommittee[finaltxnBlockDigest] = append(e.FinalBlockbyFinalCommittee[finaltxnBlockDigest], identityAndSign)
			}

			// block is signed by sufficient final members and when the final block has not been sent to the client yet
			if len(e.FinalBlockbyFinalCommittee[finaltxnBlockDigest]) >= C/2+1 && e.State != ElasticoStates["FinalBlockSentToClient"] {
				// for final members, their state is updated only when they have also sent the finalblock to ntw
				if e.IsFinalMember() {
					finalBlockSent := e.FinalBlock.Sent
					if finalBlockSent {

						e.State = ElasticoStates["FinalBlockReceived"]
					}

				} else {

					e.State = ElasticoStates["FinalBlockReceived"]
				}

			}
			// union of Commitments
			e.UnionSet(receivedCommitments)

		} else {

			logger.Error("Signature invalid in final block received")
		}
	} else {
		logger.Error("PoW not valid when final member Send the block")
	}
}

// UnionSet :-
func (e *Elastico) UnionSet(receivedSet []string) {
	logger.Info("file:- elastico.go, func:- UnionSet()")
	// logger.Info("lenn of commitment---", len(e.NewRcommitmentSet))
	// logger.Info("received set--", receivedSet)
	for _, commitment := range receivedSet {
		e.NewRcommitmentSet[commitment] = true
	}
	// logger.Info("new lenn of commitment---", len(e.NewRcommitmentSet))
}

// ResetMsg - Reset msg
type ResetMsg struct {
	Identity IDENTITY
}

// NotifyFinalMsg - Notify final members
type NotifyFinalMsg struct {
	Identity IDENTITY
}

// FinalpbftProcessMessage :- Process the messages related to Pbft!
func (e *Elastico) FinalpbftProcessMessage(msg DecodeMsgType) {

	logger.Info("file:- elastico.go, func:- FinalpbftProcessMessage()")
	if msg.Type == "Finalpre-prepare" {
		e.ProcessFinalprePrepareMsg(msg)

	} else if msg.Type == "Finalprepare" {
		e.ProcessFinalprepareMsg(msg)

	} else if msg.Type == "Finalcommit" {
		e.ProcessFinalcommitMsg(msg)
	}
}

// ProcessPrePrepareMsg :-
func (e *Elastico) ProcessPrePrepareMsg(msg DecodeMsgType) {
	/*
		Process Pre-Prepare msg
	*/
	var decodeMsg PrePrepareMsg

	err := json.Unmarshal(msg.Data, &decodeMsg)
	FailOnError(err, "fail to decode pre-prepare msg", true)

	// verify the pre-prepare message

	verified := e.VerifyPrePrepare(decodeMsg)
	if verified {
		// Log the pre-prepare msgs!
		logger.Info("pre-prepared verified by port--", e.Port)
		e.LogPrePrepareMsg(decodeMsg)

	} else {
		logger.Error("error in verification of ProcessPrePrepareMsg")
	}
}

// VerifySign :-
func (e *Elastico) VerifySign(signature string, digest []byte, PublicKey *rsa.PublicKey) bool {
	/*
		verify whether signature is valid or not
	*/
	signed, err := base64.StdEncoding.DecodeString(signature) // Decode the base64 encoded signature
	FailOnError(err, "Decode error of signature", true)
	err = rsa.VerifyPKCS1v15(PublicKey, crypto.SHA256, digest, signed) // verify the Sign of digest
	if err != nil {
		return false
	}
	return true
}

// VerifyPrePrepare :-
func (e *Elastico) VerifyPrePrepare(msg PrePrepareMsg) bool {
	/*
		Verify pre-prepare msgs
	*/
	Identityobj := msg.Identity
	prePreparedData := msg.PrePrepareData
	txnBlockList := msg.Message
	// verify Pow
	if e.VerifyPoW(Identityobj) == false {

		logger.Error("wrong pow in  verify pre-prepare")
		return false
	}
	// verify signatures of the received msg
	Sign := msg.Sign
	prePreparedDataDigest := e.DigestPrePrepareMsg(prePreparedData)
	PK := Identityobj.PK
	if e.VerifySign(Sign, prePreparedDataDigest, &PK) == false {

		logger.Error("wrong Sign in  verify pre-prepare")
		return false
	}
	// verifying the digest of request msg
	prePreparedDataTxnDigest := prePreparedData.Digest
	if txnHexdigest(txnBlockList) != prePreparedDataTxnDigest {

		logger.Error("wrong digest in  verify pre-prepare")
		return false
	}
	// check the view is same or not
	prePreparedDataView := prePreparedData.ViewID
	if prePreparedDataView != e.ViewID {

		logger.Error("wrong view in  verify pre-prepare")
		return false
	}
	// check if already accepted a pre-prepare msg for view v and sequence num n with different digest
	seqnum := prePreparedData.Seq
	for socket := range e.PrePrepareMsgLog {

		prePrepareMsgLogSocket := e.PrePrepareMsgLog[socket]
		prePrepareMsgLogData := prePrepareMsgLogSocket.PrePrepareData
		if prePrepareMsgLogData.ViewID == e.ViewID && prePrepareMsgLogData.Seq == seqnum {

			if prePreparedDataTxnDigest != prePrepareMsgLogData.Digest {

				return false
			}
		}
	}
	// If msg is discarded then what to do
	return true
}

// ProcessFinalprePrepareMsg :-
func (e *Elastico) ProcessFinalprePrepareMsg(msg DecodeMsgType) {
	/*
		Process Final Pre-Prepare msg
	*/
	var decodeMsg PrePrepareMsg

	err := json.Unmarshal(msg.Data, &decodeMsg)
	FailOnError(err, "fail to decode final pre-prepare msg", true)

	logger.Info("final pre-prepare msg of port", e.Port, "msg--", decodeMsg)
	// verify the Final pre-prepare message
	verified := e.VerifyFinalPrePrepare(decodeMsg)
	if verified {
		logger.Info("final pre-prepare verified")
		// Log the final pre-prepare msgs!
		e.LogFinalPrePrepareMsg(decodeMsg)

	} else {
		logger.Error("error in verification of Final ProcessPrePrepareMsg")
	}
}

// VerifyFinalPrePrepare :-
func (e *Elastico) VerifyFinalPrePrepare(msg PrePrepareMsg) bool {
	/*
		Verify final pre-prepare msgs
	*/
	Identityobj := msg.Identity
	prePreparedData := msg.PrePrepareData
	txnBlockList := msg.Message

	// verify Pow
	if e.VerifyPoW(Identityobj) == false {

		logger.Warn("wrong pow in  verify final pre-prepare")
		return false
	}
	// verify signatures of the received msg
	Sign := msg.Sign
	prePreparedDataDigest := e.DigestPrePrepareMsg(prePreparedData)
	PK := Identityobj.PK
	if e.VerifySign(Sign, prePreparedDataDigest, &PK) == false {

		logger.Warn("wrong Sign in  verify final pre-prepare")
		return false
	}

	// verifying the digest of request msg
	prePreparedDataTxnDigest := prePreparedData.Digest
	if txnHexdigest(txnBlockList) != prePreparedDataTxnDigest {

		logger.Warn("wrong digest in  verify final pre-prepare")
		return false
	}
	// check the view is same or not
	prePreparedDataView := prePreparedData.ViewID
	if prePreparedDataView != e.ViewID {

		logger.Warn("wrong view in  verify final pre-prepare")
		return false
	}
	// check if already accepted a pre-prepare msg for view v and sequence num n with different digest
	seqnum := prePreparedData.Seq
	for socket := range e.FinalPrePrepareMsgLog {

		prePrepareMsgLogSocket := e.PrePrepareMsgLog[socket]
		prePrepareMsgLogData := prePrepareMsgLogSocket.PrePrepareData

		if prePrepareMsgLogData.ViewID == e.ViewID && prePrepareMsgLogData.Seq == seqnum {

			if prePreparedDataTxnDigest != prePrepareMsgLogData.Digest {

				return false
			}
		}
	}
	return true

}

// VerifyCommit :-
func (e *Elastico) VerifyCommit(msg CommitMsg) bool {
	/*
		verify commit msgs
	*/
	// verify Pow
	Identityobj := msg.Identity
	if !e.VerifyPoW(Identityobj) {
		return false
	}
	// verify signatures of the received msg

	Sign := msg.Sign
	commitData := msg.CommitData
	digestCommitData := e.DigestCommitMsg(commitData)
	PK := Identityobj.PK
	if !e.VerifySign(Sign, digestCommitData, &PK) {
		return false
	}

	// check the view is same or not
	ViewID := commitData.ViewID
	if ViewID != e.ViewID {
		return false
	}
	return true
}

// LogCommitMsg :-
func (e *Elastico) LogCommitMsg(msg CommitMsg) {
	/*
	 log the commit msg
	*/
	Identityobj := msg.Identity
	commitContents := msg.CommitData
	ViewID := commitContents.ViewID
	seqnum := commitContents.Seq
	socketID := msg.Identity.IP + ":" + msg.Identity.Port

	// add msgs for this view
	if _, ok := e.CommitMsgLog[ViewID]; ok == false {

		e.CommitMsgLog[ViewID] = make(map[int]map[string][]CommitMsgData)
	}

	commitMsgLogViewID := e.CommitMsgLog[ViewID]
	// add msgs for this sequence num
	if _, ok := commitMsgLogViewID[seqnum]; ok == false {

		commitMsgLogViewID[seqnum] = make(map[string][]CommitMsgData)
	}

	// add all msgs from this sender
	commitMsgLogSeq := commitMsgLogViewID[seqnum]
	if _, ok := commitMsgLogSeq[socketID]; ok == false {

		commitMsgLogSeq[socketID] = make([]CommitMsgData, 0)
	}

	// log only required details from the commit msg
	commitDataDigest := commitContents.Digest
	msgDetails := CommitMsgData{Digest: commitDataDigest, Identity: Identityobj}
	// append msg
	commitMsgLogSocket := commitMsgLogSeq[socketID]
	commitMsgLogSocket = append(commitMsgLogSocket, msgDetails)
	e.CommitMsgLog[ViewID][seqnum][socketID] = commitMsgLogSocket
}

// ProcessCommitMsg :-
func (e *Elastico) ProcessCommitMsg(msg DecodeMsgType) {
	/*
		process the commit msg
	*/
	// verify the commit message
	var decodeMsg CommitMsg

	err := json.Unmarshal(msg.Data, &decodeMsg)
	FailOnError(err, "fail to decode commit msg", true)

	logger.Info("commit msg in--", e.Port, "msg -- ", decodeMsg)
	verified := e.VerifyCommit(decodeMsg)
	if verified {
		logger.Info("commit verified")
		// Log the commit msgs!
		e.LogCommitMsg(decodeMsg)
	}
}

// ProcessFinalcommitMsg :-
func (e *Elastico) ProcessFinalcommitMsg(msg DecodeMsgType) {
	/*
		process the final commit msg
	*/
	// verify the commit message
	var decodeMsg CommitMsg

	err := json.Unmarshal(msg.Data, &decodeMsg)
	FailOnError(err, "fail to decode final commit msg", true)

	logger.Info("final commit msg in port--", e.Port, "with msg--", decodeMsg)
	verified := e.VerifyCommit(decodeMsg)
	if verified {
		fmt.Println("final commit verified")
		// Log the commit msgs!
		e.LogFinalCommitMsg(decodeMsg)
	}
}

// LogFinalCommitMsg :-
func (e *Elastico) LogFinalCommitMsg(msg CommitMsg) {
	/*
		log the final commit msg
	*/
	Identityobj := msg.Identity
	commitContents := msg.CommitData
	ViewID := commitContents.ViewID
	seqnum := commitContents.Seq

	socketID := Identityobj.IP + ":" + Identityobj.Port
	// add msgs for this view
	_, ok := e.FinalcommitMsgLog[ViewID]
	if ok == false {

		e.FinalcommitMsgLog[ViewID] = make(map[int]map[string][]CommitMsgData)
	}
	commitMsgLogViewID := e.FinalcommitMsgLog[ViewID]
	// add msgs for this sequence num
	_, okk := commitMsgLogViewID[seqnum]
	if okk == false {

		commitMsgLogViewID[seqnum] = make(map[string][]CommitMsgData)
	}

	commitMsgLogSeq := commitMsgLogViewID[seqnum]
	// add all msgs from this sender
	_, okkk := commitMsgLogSeq[socketID]
	if okkk == false {

		commitMsgLogSeq[socketID] = make([]CommitMsgData, 0)
	}

	// log only required details from the commit msg
	commitDataDigest := commitContents.Digest
	msgDetails := CommitMsgData{Digest: commitDataDigest, Identity: Identityobj}
	// append msg
	commitMsgLogSocket := commitMsgLogSeq[socketID]
	commitMsgLogSocket = append(commitMsgLogSocket, msgDetails)
	e.FinalcommitMsgLog[ViewID][seqnum][socketID] = commitMsgLogSocket

}

// PbftProcessMessage :-
func (e *Elastico) PbftProcessMessage(msg DecodeMsgType) {
	/*
		Process the messages related to Pbft!
	*/
	if msg.Type == "pre-prepare" {

		e.ProcessPrePrepareMsg(msg)

	} else if msg.Type == "prepare" {

		e.ProcessPrepareMsg(msg)

	} else if msg.Type == "commit" {
		e.ProcessCommitMsg(msg)
	}
}

// ProcessPrepareMsg :-
func (e *Elastico) ProcessPrepareMsg(msg DecodeMsgType) {
	/*
		process prepare msg
	*/
	// verify the prepare message
	var decodeMsg PrepareMsg

	err := json.Unmarshal(msg.Data, &decodeMsg)
	FailOnError(err, "fail to decode prepare msg", true)
	logger.Infof("prepare msg in-- %s msg---", e.Port)

	verified := e.VerifyPrepare(decodeMsg)
	if verified {
		logger.Info("prepare verified")
		// Log the prepare msgs!
		e.LogPrepareMsg(decodeMsg)
	}
}

// LogPrepareMsg :-
func (e *Elastico) LogPrepareMsg(msg PrepareMsg) {
	/*
		log the prepare msg
	*/
	Identityobj := msg.Identity
	prepareData := msg.PrepareData
	ViewID := prepareData.ViewID
	seqnum := prepareData.Seq

	socketID := Identityobj.IP + ":" + Identityobj.Port
	// add msgs for this view
	if _, ok := e.PrepareMsgLog[ViewID]; ok == false {

		e.PrepareMsgLog[ViewID] = make(map[int]map[string][]PrepareMsgData)
	}
	prepareMsgLogViewID := e.PrepareMsgLog[ViewID]
	// add msgs for this sequence num
	if _, ok := prepareMsgLogViewID[seqnum]; ok == false {

		prepareMsgLogViewID[seqnum] = make(map[string][]PrepareMsgData)
	}

	// add all msgs from this sender
	prepareMsgLogSeq := prepareMsgLogViewID[seqnum]
	if _, ok := prepareMsgLogSeq[socketID]; ok == false {

		prepareMsgLogSeq[socketID] = make([]PrepareMsgData, 0)
	}

	// ToDo: check that the msg appended is dupicate or not
	// log only required details from the prepare msg
	prepareDataDigest := prepareData.Digest
	msgDetails := PrepareMsgData{Digest: prepareDataDigest, Identity: Identityobj}
	// append msg to prepare msg log
	prepareMsgLogSocket := prepareMsgLogSeq[socketID]
	prepareMsgLogSocket = append(prepareMsgLogSocket, msgDetails)
	e.PrepareMsgLog[ViewID][seqnum][socketID] = prepareMsgLogSocket

}

// VerifyPrepare :-
func (e *Elastico) VerifyPrepare(msg PrepareMsg) bool {
	/*
	 Verify prepare msgs
	*/
	// verify Pow
	Identityobj := msg.Identity
	if e.VerifyPoW(Identityobj) == false {

		logger.Error("wrong pow in verify prepares")
		return false
	}

	// verify signatures of the received msg
	Sign := msg.Sign
	prepareData := msg.PrepareData
	PrepareContentsDigest := e.DigestPrepareMsg(prepareData)
	PK := Identityobj.PK
	if e.VerifySign(Sign, PrepareContentsDigest, &PK) == false {

		logger.Error("wrong Sign in verify prepares")
		return false
	}

	// check the view is same or not
	ViewID := prepareData.ViewID
	seq := prepareData.Seq
	digest := prepareData.Digest
	if ViewID != e.ViewID {

		logger.Error("wrong view in verify prepares")
		return false
	}
	// verifying the digest of request msg
	for socketID := range e.PrePrepareMsgLog {

		prePrepareMsg := e.PrePrepareMsgLog[socketID]
		prePrepareData := prePrepareMsg.PrePrepareData

		if prePrepareData.ViewID == ViewID && prePrepareData.Seq == seq && prePrepareData.Digest == digest {
			return true
		}
	}
	return false
}

// VerifyFinalPrepare :-
func (e *Elastico) VerifyFinalPrepare(msg PrepareMsg) bool {
	/*
		Verify final prepare msgs
	*/
	// verify Pow
	Identityobj := msg.Identity
	if e.VerifyPoW(Identityobj) == false {

		logger.Warn("wrong pow in verify final prepares")
		return false
	}
	// verify signatures of the received msg
	Sign := msg.Sign
	prepareData := msg.PrepareData
	PrepareContentsDigest := e.DigestPrepareMsg(prepareData)

	PK := Identityobj.PK

	if e.VerifySign(Sign, PrepareContentsDigest, &PK) == false {

		logger.Warn("wrong Sign in verify final prepares")
		return false
	}
	ViewID := prepareData.ViewID
	seq := prepareData.Seq
	digest := prepareData.Digest
	// check the view is same or not
	if ViewID != e.ViewID {

		logger.Warn("wrong view in verify final prepares")
		return false
	}

	// verifying the digest of request msg
	for socketID := range e.FinalPrePrepareMsgLog {
		prePrepareMsg := e.FinalPrePrepareMsgLog[socketID]
		prePrepareData := prePrepareMsg.PrePrepareData

		if prePrepareData.ViewID == ViewID && prePrepareData.Seq == seq && prePrepareData.Digest == digest {

			return true
		}
	}
	return false
}

// ProcessFinalprepareMsg :-
func (e *Elastico) ProcessFinalprepareMsg(msg DecodeMsgType) {
	/*
		process final prepare msg
	*/
	var decodeMsg PrepareMsg
	err := json.Unmarshal(msg.Data, &decodeMsg)
	FailOnError(err, "fail to decode final prepare msg", true)
	// verify the prepare message

	logger.Info("final prepare msg of port--", e.Port, "with msg--", decodeMsg)
	verified := e.VerifyFinalPrepare(decodeMsg)
	if verified {
		fmt.Println("FINAL PREPARE VERIFIED with port--", e.Port)
		// Log the prepare msgs!
		e.LogFinalPrepareMsg(decodeMsg)
	}
}

// LogFinalPrepareMsg :-
func (e *Elastico) LogFinalPrepareMsg(msg PrepareMsg) {
	/*
		log the prepare msg
	*/
	Identityobj := msg.Identity
	prepareData := msg.PrepareData
	ViewID := prepareData.ViewID
	seqnum := prepareData.Seq

	socketID := Identityobj.IP + ":" + Identityobj.Port

	// add msgs for this view
	if _, ok := e.FinalPrepareMsgLog[ViewID]; ok == false {

		e.FinalPrepareMsgLog[ViewID] = make(map[int]map[string][]PrepareMsgData)
	}

	prepareMsgLogViewID := e.FinalPrepareMsgLog[ViewID]
	// add msgs for this sequence num
	if _, ok := prepareMsgLogViewID[seqnum]; ok == false {

		prepareMsgLogViewID[seqnum] = make(map[string][]PrepareMsgData)
	}

	// add all msgs from this sender
	prepareMsgLogSeq := prepareMsgLogViewID[seqnum]
	if _, ok := prepareMsgLogSeq[socketID]; ok == false {

		prepareMsgLogSeq[socketID] = make([]PrepareMsgData, 0)
	}

	// ToDo: check that the msg appended is dupicate or not
	// log only required details from the prepare msg
	prepareDataDigest := prepareData.Digest
	msgDetails := PrepareMsgData{Digest: prepareDataDigest, Identity: Identityobj}
	// append msg to prepare msg log
	prepareMsgLogSocket := prepareMsgLogSeq[socketID]
	prepareMsgLogSocket = append(prepareMsgLogSocket, msgDetails)
	e.FinalPrepareMsgLog[ViewID][seqnum][socketID] = prepareMsgLogSocket
}

// UnionViews :-
func (e *Elastico) UnionViews(nodeData, incomingData []IDENTITY) []IDENTITY {
	/*
		nodeData and incomingData are the set of identities
		Adding those identities of incomingData to nodeData that are not present in nodeData
	*/
	for _, data := range incomingData {

		Flag := false
		for _, nodeID := range nodeData {

			// data is present already in nodeData
			if nodeID.IsEqual(&data) {
				Flag = true
				break
			}
		}
		if Flag == false {
			// Adding the new Identity
			nodeData = append(nodeData, data)
		}
	}
	return nodeData
}

// ViewsMsg - Views of committees
type ViewsMsg struct {
	CommitteeMembers      []IDENTITY
	FinalCommitteeMembers []IDENTITY
	Identity              IDENTITY
	Txns                  []Transaction
}

// ReceiveViews :-
func (e *Elastico) ReceiveViews(msg DecodeMsgType) {
	var decodeMsg ViewsMsg

	err := json.Unmarshal(msg.Data, &decodeMsg)
	// fmt.Println("Views msg---", decodeMsg)
	FailOnError(err, "fail to decode Views msg", true)

	Identityobj := decodeMsg.Identity

	if e.VerifyPoW(Identityobj) {

		if _, ok := e.Views[Identityobj.IP+Identityobj.Port]; ok == false {

			logger.Infof("my Ip:- %s for the views", e.IP)
			// union of committe members Views
			e.Views[Identityobj.IP+Identityobj.Port] = true

			commMembers := decodeMsg.CommitteeMembers
			finalMembers := decodeMsg.FinalCommitteeMembers
			Txns := decodeMsg.Txns

			// update the Txn block
			// ToDo: txnblock should be ordered, not set
			if len(Txns) > 0 {

				e.TxnBlock = e.UnionTxns(e.TxnBlock, Txns)
				logger.Infof("I am Primary %s , %s ", e.IP, strconv.Itoa(len(e.TxnBlock[0].Txn.Payload)))
				e.Primary = true
			}

			// ToDo: verify this union thing
			// union of committee members wrt directory member
			e.CommitteeMembers = e.UnionViews(e.CommitteeMembers, commMembers)
			// union of final committee members wrt directory member
			e.FinalCommitteeMembers = e.UnionViews(e.FinalCommitteeMembers, finalMembers)
			// received the members
			if len(e.Views) >= C/2+1 {

				e.State = ElasticoStates["Receiving Committee Members"]
			}
		}
	}
}
