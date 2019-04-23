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
var D = 3

// C - size of committee
var C = 4

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

	e.Identity = IDENTITY{}
	e.CommitteeID = -1
	var Ri string
	e.Ri = Ri

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
		if strings.HasPrefix(hashVal, zeroString) {
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

	// Valid Hash has D leading '0's (in hex)
	if !strings.HasPrefix(hash, zeroString) {
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
	if strings.HasPrefix(hash, zeroString) && hashVal == hash {
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

						logger.Error("wrong digest in is committed")
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

func (e *Elastico) isPrePrepared() bool {
	/*
		if the node received the pre-prepare msg from the Primary
	*/
	logger.Info("file:- elastico.go, func:- isPrePrepared()")
	return len(e.PrePrepareMsgLog) > 0
}

func (e *Elastico) isFinalprePrepared() bool {

	logger.Info("file:- elastico.go, func:- isFinalprePrepared()")
	return len(e.FinalPrePrepareMsgLog) > 0
}

func (t *Message) hexdigest() string {
	/*
		Digest of a transaction
	*/
	logger.Info("file:- elastico.go, func:- hexdigest of Message()")
	digest := sha256.New()
	if t.ConfigMsg == nil {
		digest.Write(t.NormalMsg.Payload)
		digest.Write(t.NormalMsg.Signature)
		digest.Write(t.NormalMsg.XXX_unrecognized)
		digest.Write([]byte(strconv.FormatInt(int64(t.NormalMsg.XXX_sizecache), 10)))
	} else {
		digest.Write(t.ConfigMsg.Payload)
		digest.Write(t.ConfigMsg.Signature)
		digest.Write(t.ConfigMsg.XXX_unrecognized)
		digest.Write([]byte(strconv.FormatInt(int64(t.ConfigMsg.XXX_sizecache), 10)))
	}
	digest.Write([]byte(strconv.FormatUint(t.ConfigSeq, 10))) // convert amount(big.Int) to string

	hashVal := fmt.Sprintf("%x", digest.Sum(nil))
	return hashVal
}

// txnHexdigest - Hex digest of Txn List
func txnHexdigest(txnList []*Message) string {
	/*
		return hexdigest for a list of transactions
	*/
	logger.Info("file:- elastico.go, func:- hexdigest of Txn list()")
	// ToDo : Sort the Txns based on hash value
	txnDigestList := make([]string, len(txnList))
	for i := 0; i < len(txnList); i++ {
		txnDigest := txnList[i].hexdigest()
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

//Sign :- sign the byte array
func (e *Elastico) Sign(digest []byte) string {
	logger.Info("file:- elastico.go, func:- Sign()")
	signed, err := rsa.SignPKCS1v15(rand.Reader, e.Key, crypto.SHA256, digest) // sign the digest
	FailOnError(err, "Error in Signing byte array", true)
	signature := base64.StdEncoding.EncodeToString(signed) // encode to base64
	return signature
}

func (e *Elastico) constructPrePrepare(epoch string) map[string]interface{} {
	/*
		construct pre-prepare msg , done by Primary
	*/
	logger.Info("file:- elastico.go, func:- constructPrePrepare()")
	txnBlockList := e.TxnBlock
	// ToDo: make prePrepareContents Ordered Dict for signatures purpose
	prePrepareContents := PrePrepareContents{Type: "pre-prepare", ViewID: e.ViewID, Seq: 1, Digest: txnHexdigest(txnBlockList)}

	prePrepareContentsDigest := e.digestPrePrepareMsg(prePrepareContents)

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

func (e *Elastico) constructPrepare(epoch string) []map[string]interface{} {
	/*
		construct prepare msg in the prepare phase
	*/
	logger.Info("file:- elastico.go, func:- constructPrepare()")
	prepareMsgList := make([]map[string]interface{}, 0)
	//  loop over all pre-prepare msgs
	for socketID := range e.PrePrepareMsgLog {

		msg := e.PrePrepareMsgLog[socketID]
		prePreparedData := msg.PrePrepareData
		seqnum := prePreparedData.Seq
		digest := prePreparedData.Digest

		//  make prepare_contents Ordered Dict for signatures purpose
		prepareContents := PrepareContents{Type: "prepare", ViewID: e.ViewID, Seq: seqnum, Digest: digest}
		PrepareContentsDigest := e.digestPrepareMsg(prepareContents)
		data := map[string]interface{}{"PrepareData": prepareContents, "Sign": e.Sign(PrepareContentsDigest), "Identity": e.Identity}
		preparemsg := map[string]interface{}{"data": data, "type": "prepare", "epoch": epoch}
		prepareMsgList = append(prepareMsgList, preparemsg)
	}
	return prepareMsgList

}

func (e *Elastico) constructFinalPrepare(epoch string) []map[string]interface{} {
	/*
		construct prepare msg in the prepare phase
	*/
	logger.Info("file:- elastico.go, func:- constructFinalPrepare()")
	FinalprepareMsgList := make([]map[string]interface{}, 0)
	for socketID := range e.FinalPrePrepareMsgLog {

		msg := e.FinalPrePrepareMsgLog[socketID]
		prePreparedData := msg.PrePrepareData
		seqnum := prePreparedData.Seq
		digest := prePreparedData.Digest
		//  make prepare_contents Ordered Dict for signatures purpose

		prepareContents := PrepareContents{Type: "Finalprepare", ViewID: e.ViewID, Seq: seqnum, Digest: digest}
		PrepareContentsDigest := e.digestPrepareMsg(prepareContents)

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

func (e *Elastico) constructCommit(epoch string) []map[string]interface{} {
	/*
		Construct commit msgs
	*/
	logger.Info("file:- elastico.go, func:- constructCommit()")
	commitMsges := make([]map[string]interface{}, 0)

	for ViewID := range e.PreparedData {

		for seqnum := range e.PreparedData[ViewID] {

			digest := txnHexdigest(e.PreparedData[ViewID][seqnum])
			// make commit_contents Ordered Dict for signatures purpose
			commitContents := CommitContents{Type: "commit", ViewID: ViewID, Seq: seqnum, Digest: digest}
			commitContentsDigest := e.digestCommitMsg(commitContents)
			data := map[string]interface{}{"Sign": e.Sign(commitContentsDigest), "CommitData": commitContents, "Identity": e.Identity}
			commitMsg := map[string]interface{}{"data": data, "type": "commit", "epoch": epoch}
			commitMsges = append(commitMsges, commitMsg)

		}
	}

	return commitMsges
}

func (e *Elastico) constructFinalCommit(epoch string) []map[string]interface{} {
	/*
		Construct commit msgs
	*/
	logger.Info("file:- elastico.go, func:- constructFinalCommit()")
	commitMsges := make([]map[string]interface{}, 0)

	for ViewID := range e.FinalpreparedData {

		for seqnum := range e.FinalpreparedData[ViewID] {

			digest := txnHexdigest(e.FinalpreparedData[ViewID][seqnum])
			//  make commit_contents Ordered Dict for signatures purpose
			commitContents := CommitContents{Type: "Finalcommit", ViewID: ViewID, Seq: seqnum, Digest: digest}
			commitContentsDigest := e.digestCommitMsg(commitContents)

			data := map[string]interface{}{"Sign": e.Sign(commitContentsDigest), "CommitData": commitContents, "Identity": e.Identity}
			commitMsg := map[string]interface{}{"data": data, "type": "Finalcommit", "epoch": epoch}
			commitMsges = append(commitMsges, commitMsg)

		}
	}

	return commitMsges
}

func (e *Elastico) digestPrePrepareMsg(msg PrePrepareContents) []byte {
	logger.Info("file:- elastico.go, func:- digestPrePrepareMsg()")
	digest := sha256.New()
	digest.Write([]byte(msg.Type))
	digest.Write([]byte(strconv.Itoa(msg.ViewID)))
	digest.Write([]byte(strconv.Itoa(msg.Seq)))
	digest.Write([]byte(msg.Digest))
	return digest.Sum(nil)
}

func (e *Elastico) digestPrepareMsg(msg PrepareContents) []byte {
	logger.Info("file:- elastico.go, func:- digestPrepareMsg()")
	digest := sha256.New()
	digest.Write([]byte(msg.Type))
	digest.Write([]byte(strconv.Itoa(msg.ViewID)))
	digest.Write([]byte(strconv.Itoa(msg.Seq)))
	digest.Write([]byte(msg.Digest))
	return digest.Sum(nil)
}

func (e *Elastico) digestCommitMsg(msg CommitContents) []byte {
	logger.Info("file:- elastico.go, func:- digestCommitMsg()")
	digest := sha256.New()
	digest.Write([]byte(msg.Type))
	digest.Write([]byte(strconv.Itoa(msg.ViewID)))
	digest.Write([]byte(strconv.Itoa(msg.Seq)))
	digest.Write([]byte(msg.Digest))
	return digest.Sum(nil)
}

func (e *Elastico) constructFinalPrePrepare(epoch string) map[string]interface{} {
	/*
		construct pre-prepare msg , done by Primary final
	*/
	logger.Info("file:- elastico.go, func:- constructFinalPrePrepare()")
	txnBlockList := e.MergedBlock
	// ToDo :- make pre_prepare_contents Ordered Dict for signatures purpose
	prePrepareContents := PrePrepareContents{Type: "Finalpre-prepare", ViewID: e.ViewID, Seq: 1, Digest: txnHexdigest(txnBlockList)}

	prePrepareContentsDigest := e.digestPrePrepareMsg(prePrepareContents)

	data := map[string]interface{}{"Message": txnBlockList, "PrePrepareData": prePrepareContents, "Sign": e.Sign(prePrepareContentsDigest), "Identity": e.Identity}
	prePrepareMsg := map[string]interface{}{"data": data, "type": "Finalpre-prepare", "epoch": epoch}
	return prePrepareMsg

}

func (e *Elastico) sendPrePrepare(prePrepareMsg map[string]interface{}) {
	/*
		Send pre-prepare msgs to all committee members
	*/
	logger.Info("file:- elastico.go, func:- sendPrePrepare()")
	for _, nodeID := range e.CommitteeMembers {

		// dont send pre-prepare msg to self
		if e.Identity.isEqual(&nodeID) == false {

			nodeID.send(prePrepareMsg)
		}
	}
}

func (e *Elastico) sendCommit(commitMsgList []map[string]interface{}) {
	/*
		send the commit msgs to the committee members
	*/
	logger.Info("file:- elastico.go, func:- sendCommit()")
	for _, commitMsg := range commitMsgList {

		for _, nodeID := range e.CommitteeMembers {

			nodeID.send(commitMsg)
		}
	}
}

func (e *Elastico) sendPrepare(prepareMsgList []map[string]interface{}) {
	/*
		send the prepare msgs to the committee members
	*/
	logger.Info("file:- elastico.go, func:- sendPrepare()")
	// send prepare msg list to committee members
	for _, preparemsg := range prepareMsgList {

		for _, nodeID := range e.CommitteeMembers {

			nodeID.send(preparemsg)
		}
	}
}

// DecodeMsgType :-
type DecodeMsgType struct {
	Type  string
	Data  json.RawMessage
	Epoch string
}

// NewEpochMsg   :-
type NewEpochMsg struct {
	Data *Message
}

func (e *Elastico) receiveTxns(epochTxn NewEpochMsg) {
	/*
		directory node will receive transactions from client
	*/
	logger.Info("file:- elastico.go, func:- receiveTxns()")
	// Receive txns from client for an epoch
	// var k int64
	numOfCommittees := int64(math.Pow(2, float64(s)))
	// var num int64
	// num = 1 // Transactions per committee
	// loop in sorted order of committee ids
	var iden int64
	for iden = 0; iden < numOfCommittees; iden++ {
		e.Txn[iden] = make([]*Message, 1)
		e.Txn[iden][0] = epochTxn.Data
	}
}

func (e *Elastico) isFinalMember() bool {
	/*
		tell whether this node is a final committee member or not
	*/
	// logger.Info("file:- elastico.go, func:- isFinalMember()")
	return e.IsFinal
}

// SendtoFinal :- Each committee member sends the signed value(Txn block after intra committee consensus along with signatures to final committee
func (e *Elastico) SendtoFinal(epoch string) {

	logger.Info("file:- elastico.go, func:- SendtoFinal()")
	for ViewID := range e.CommittedData {
		committedDataViewID := e.CommittedData[ViewID]
		for seqnum := range committedDataViewID {

			msgList := committedDataViewID[seqnum]

			e.TxnBlock = e.unionTxns(e.TxnBlock, msgList)
		}
	}
	// logger.Warn("size of fin committee members", len(e.FinalCommitteeMembers))
	// logger.Warn("size of txns in Txn block", len(e.TxnBlock))
	for _, finalID := range e.FinalCommitteeMembers {

		//  here TxnBlock is a set, since sets are unordered hence can't sign them. So convert set to list for signing
		TxnBlock := e.TxnBlock
		data := map[string]interface{}{"Txnblock": TxnBlock, "Sign": e.signTxnList(TxnBlock), "Identity": e.Identity}
		msg := map[string]interface{}{"data": data, "type": "intraCommitteeBlock", "epoch": epoch}
		finalID.send(msg)
	}
	e.State = ElasticoStates["Intra Consensus Result Sent to Final"]
}

func (e *Elastico) signTxnList(TxnBlock []*Message) string {
	logger.Info("file:- elastico.go, func:- signTxnList()")
	// Sign the array of Transactions
	digest := sha256.New()
	for i := 0; i < len(TxnBlock); i++ {
		txnDigest := TxnBlock[i].hexdigest() // Get the transaction digest
		digest.Write([]byte(txnDigest))
	}
	signed, err := rsa.SignPKCS1v15(rand.Reader, e.Key, crypto.SHA256, digest.Sum(nil)) // sign the digest of Txn List
	FailOnError(err, "Error in Signing Txn List", true)
	signature := base64.StdEncoding.EncodeToString(signed) // encode to base64
	return signature
}

func (e *Elastico) checkCountForConsensusData() {
	/*
		check the sufficient count for consensus data
	*/
	logger.Info("file:- elastico.go, func:- checkCountForConsensusData()")
	Flag := false
	var commID int64
	for commID = 0; commID < int64(math.Pow(2, float64(s))); commID++ {

		if _, ok := e.CommitteeConsensusData[commID]; ok == false {

			Flag = true
			break

		} else {

			for txnBlockDigest := range e.CommitteeConsensusData[commID] {

				if len(e.CommitteeConsensusData[commID][txnBlockDigest]) <= c/2 {
					Flag = true
					logger.Warn("bad committee id for intra committee block")
					break
				}
			}
		}
	}
	if Flag == false {

		// when sufficient number of blocks from each committee are received
		logger.Info("good going for verify and merge")
		e.verifyAndMergeConsensusData()
	}

}

func (e *Elastico) verifyAndMergeConsensusData() {
	/*
		each final committee member validates that the values received from the committees are signed by
		atleast c/2 + 1 members of the proper committee and takes the ordered set union of all the inputs

	*/
	logger.Info("file:- elastico.go, func:- verifyAndMergeConsensusData()")
	var committeeid int64
	for committeeid = 0; committeeid < int64(math.Pow(2, float64(s))); committeeid++ {

		if _, presentCommID := e.CommitteeConsensusData[committeeid]; presentCommID == true {

			for txnBlockDigest := range e.CommitteeConsensusData[committeeid] {

				if len(e.CommitteeConsensusData[committeeid][txnBlockDigest]) >= c/2+1 {

					// get the txns from the digest
					TxnBlock := e.CommitteeConsensusDataTxns[committeeid][txnBlockDigest]
					if len(TxnBlock) > 0 {

						e.MergedBlock = e.unionTxns(e.MergedBlock, TxnBlock)
					}
				}
			}
		}
	}
	if len(e.MergedBlock) > 0 {
		fmt.Println("final committee port - ", e.Port, "has merged data")
		e.State = ElasticoStates["Merged Consensus Data"]
	}
}

// RunInteractiveConsistency :-
func (e *Elastico) RunInteractiveConsistency(epoch string) {
	/*
		send the H(Ri) to the final committe members.This is done by a final committee member
	*/
	logger.Info("file:- elastico.go, func:- RunInteractiveConsistency()")
	if e.isFinalMember() == true {
		for _, nodeID := range e.CommitteeMembers {

			logger.Warnf("sent the Int. Commitments  %s to %s", e.Port, nodeID.Port)
			Commitments := mapToList(e.Commitments)
			data := map[string]interface{}{"Identity": e.Identity, "Commitments": Commitments}
			msg := map[string]interface{}{"data": data, "type": "InteractiveConsistency", "epoch": epoch}
			nodeID.send(msg)
		}
		e.State = ElasticoStates["InteractiveConsistencyStarted"]
	}
}

//Execute :-
func (e *Elastico) Execute(exchangeName string, epoch string, Txn NewEpochMsg) string {
	/*
		executing the functions based on the running state
	*/
	config := EState{}
	// initial state of elastico node
	if e.State == ElasticoStates["NONE"] {
		e.executePoW()
	} else if e.State == ElasticoStates["PoW Computed"] {

		// form Identity, when PoW computed
		e.formIdentity()
	} else if e.State == ElasticoStates["Formed Identity"] {

		// form committee, when formed Identity
		e.formCommittee(exchangeName, epoch)
	} else if e.IsDirectory && e.State == ElasticoStates["RunAsDirectory"] {

		logger.Infof("The directory member :- %s ", e.IP)
		e.receiveTxns(Txn)
		// directory member has received the txns for all committees
		e.State = ElasticoStates["RunAsDirectory after-TxnReceived"]
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
		e.runPBFT(epoch)

	} else if e.State == ElasticoStates["PBFT_NONE"] || e.State == ElasticoStates["PBFT_PRE_PREPARE"] || e.State == ElasticoStates["PBFT_PREPARE_SENT"] || e.State == ElasticoStates["PBFT_PREPARED"] || e.State == ElasticoStates["PBFT_COMMIT_SENT"] || e.State == ElasticoStates["PBFT_PRE_PREPARE_SENT"] {

		// run pbft for intra consensus
		e.runPBFT(epoch)
	} else if e.State == ElasticoStates["PBFT_COMMITTED"] {

		// send pbft consensus blocks to final committee members
		logger.Infof("pbft finished by members %s", e.Port)
		e.SendtoFinal(epoch)

	} else if e.isFinalMember() && e.State == ElasticoStates["Intra Consensus Result Sent to Final"] {

		// final committee node will collect blocks and merge them
		e.checkCountForConsensusData()

	} else if e.isFinalMember() && e.State == ElasticoStates["Merged Consensus Data"] {

		// final committee member runs final pbft
		e.State = ElasticoStates["FinalPBFT_NONE"]
		fmt.Println("start pbft by final member with port--", e.Port)
		e.runFinalPBFT(epoch)

	} else if e.State == ElasticoStates["FinalPBFT_NONE"] || e.State == ElasticoStates["FinalPBFT_PRE_PREPARE"] || e.State == ElasticoStates["FinalPBFT_PREPARE_SENT"] || e.State == ElasticoStates["FinalPBFT_PREPARED"] || e.State == ElasticoStates["FinalPBFT_COMMIT_SENT"] || e.State == ElasticoStates["FinalPBFT_PRE_PREPARE_SENT"] {

		e.runFinalPBFT(epoch)

	} else if e.isFinalMember() && e.State == ElasticoStates["FinalPBFT_COMMITTED"] {

		// send the commitment to other final committee members
		e.sendCommitment(epoch)
		logger.Warn("pbft finished by final committee", e.Port)

	} else if e.isFinalMember() && e.State == ElasticoStates["CommitmentSentToFinal"] {

		// broadcast final Txn block to ntw
		if len(e.Commitments) >= c/2+1 {
			logger.Info("Commitments received sucess")
			e.RunInteractiveConsistency(epoch)
		}
	} else if e.isFinalMember() && e.State == ElasticoStates["InteractiveConsistencyStarted"] {
		if len(e.EpochcommitmentSet) >= c/2+1 {
			e.State = ElasticoStates["InteractiveConsistencyAchieved"]
		} else {
			logger.Warn("Int. Consistency short : ", len(e.EpochcommitmentSet), " port :", e.Port)
		}
	} else if e.isFinalMember() && e.State == ElasticoStates["InteractiveConsistencyAchieved"] {

		// broadcast final Txn block to ntw
		logger.Info("Consistency received sucess")
		e.BroadcastFinalTxn(epoch, exchangeName)
	} else if e.State == ElasticoStates["FinalBlockReceived"] {

		e.checkCountForFinalData()

	} else if e.isFinalMember() && e.State == ElasticoStates["FinalBlockSentToClient"] {

		// broadcast Ri is done when received commitment has atleast c/2  + 1 signatures
		if len(e.NewRcommitmentSet) >= c/2+1 {
			logger.Info("broadacst R by port--", e.Port)
			e.BroadcastR(epoch, exchangeName)
		} else {
			logger.Info("insufficient Rs")
		}
	} else if e.State == ElasticoStates["BroadcastedR"] {
		if len(e.NewsetOfRs) >= c/2+1 {
			logger.Info("received the set of Rs")
			e.State = ElasticoStates["ReceivedR"]
		} else {
			logger.Info("Insuffice Set of Rs")
		}
	} else if e.State == ElasticoStates["ReceivedR"] {

		e.State = ElasticoStates["Reset"]
		config.State = strconv.Itoa(ElasticoStates["Reset"])
		SetState(config, "/conf.json")
		// Now, the node can be reset
		return "reset"
	}
	config.State = strconv.Itoa(e.State)
	SetState(config, "/conf.json")
	return ""
}

func (e *Elastico) digestCommitments(receivedCommitments []string) []byte {
	logger.Info("file:- elastico.go, func:- digestCommitments()")
	digest := sha256.New()
	for _, commitment := range receivedCommitments {
		digest.Write([]byte(commitment))
	}
	return digest.Sum(nil)
}

func mapToList(m map[string]bool) []string {
	logger.Info("file:- elastico.go, func:- mapToList()")
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
	logger.Info("file:- elastico.go, func:- BroadcastFinalTxn()")
	commitmentList := mapToList(e.EpochcommitmentSet)
	commitmentDigest := e.digestCommitments(commitmentList)
	data := map[string]interface{}{"CommitSet": commitmentList, "Signature": e.Sign(commitmentDigest), "Identity": e.Identity, "FinalBlock": e.FinalBlock.Txns, "FinalBlockSign": e.signTxnList(e.FinalBlock.Txns)}
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

func (e *Elastico) generateRandomstrings() {
	/*
		Generate r-bit random strings
	*/
	logger.Info("file:- elastico.go, func:- generateRandomstrings()")
	if e.isFinalMember() {
		Ri := RandomGen(r)
		e.Ri = fmt.Sprintf("%0"+strconv.FormatInt(r, 10)+"b\n", Ri)
	}
}

func (e *Elastico) getCommitment() string {
	/*
		generate commitment for random string Ri. This is done by a final committee member
	*/
	logger.Info("file:- elastico.go, func:- getCommitment()")
	if e.Ri == "" {
		e.generateRandomstrings()
	}
	commitment := sha256.New()
	commitment.Write([]byte(e.Ri))
	hashVal := fmt.Sprintf("%x", commitment.Sum(nil))
	logger.Infof("commitment Ri-- %s of port %s", e.Ri, e.Port)
	logger.Infof("Commitments H(Ri)-- %s", hashVal)
	return hashVal
}

func (e *Elastico) sendCommitment(epoch string) {
	/*
		send the H(Ri) to the final committe members.This is done by a final committee member
	*/
	logger.Info("file:- elastico.go, func:- sendCommitment()")
	if e.isFinalMember() == true {

		HashRi := e.getCommitment()
		for _, nodeID := range e.CommitteeMembers {

			// logger.Warn("sent the commitment by ", e.Port, " to ", nodeID.Port)
			data := map[string]interface{}{"Identity": e.Identity, "HashRi": HashRi}
			msg := map[string]interface{}{"data": data, "type": "hash", "epoch": epoch}
			nodeID.send(msg)
		}
		e.State = ElasticoStates["CommitmentSentToFinal"]
	}
}

func (e *Elastico) checkCountForFinalData() {
	/*
		check the sufficient counts for final data
	*/
	//  collect final blocks sent by final committee and add the blocks to the Response

	logger.Info("file:- elastico.go, func:- checkCountForFinalData()")
	for txnBlockDigest := range e.FinalBlockbyFinalCommittee {

		if len(e.FinalBlockbyFinalCommittee[txnBlockDigest]) >= c/2+1 {

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

		logger.Warnf("final block sent the block to client by %s", e.Port)
		e.State = ElasticoStates["FinalBlockSentToClient"]
	}
}

// BroadcastR :- broadcast Ri to all the network, final member will do this
func (e *Elastico) BroadcastR(epoch string, exchangeName string) {
	logger.Info("file:- elastico.go, func:- BroadcastR()")
	if e.isFinalMember() {
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
func (e *Elastico) Consume(channel *amqp.Channel, queueName string, Txn NewEpochMsg, epoch string) {
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
					// consume the msg by taking the action in receive
					e.receive(decodedmsg, epoch)
				} //else if decodedmsg.Epoch > epoch {
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

func (i *IDENTITY) isEqual(identityobj *IDENTITY) bool {
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

	// listOfRsIniobj := identityobj.PoW["SetOfRs"].([]interface{})
	// setOfRsIniobj := make([]string, len(listOfRsIniobj))
	// for i := range listOfRsIniobj {
	// 	setOfRsIniobj[i] = listOfRsIniobj[i].(string)
	// }
	// logger.Info("file:- elastico.go, func:- isEqual of identity()")
	if i.PK.N.Cmp(identityobj.PK.N) != 0 {
		return false
	}
	return i.IP == identityobj.IP && i.PK.E == identityobj.PK.E && i.CommitteeID == identityobj.CommitteeID && i.PoW.Hash == identityobj.PoW.Hash && i.PoW.Nonce == identityobj.PoW.Nonce && i.EpochRandomness == identityobj.EpochRandomness && i.Port == identityobj.Port
}

// Dmsg :- structure for directory msg
type Dmsg struct {
	Identity IDENTITY
}

func (e *Elastico) receiveDirectoryMember(msg DecodeMsgType) {
	logger.Infof("file:- elastico.go, func:- receiveDirectoryMember() by %s", e.IP)
	var decodeMsg Dmsg

	err := json.Unmarshal(msg.Data, &decodeMsg)
	FailOnError(err, "fail to decode directory member msg", true)

	identityobj := decodeMsg.Identity
	// verify the PoW of the sender
	if e.verifyPoW(identityobj) {
		if len(e.CurDirectory) < c {
			// check whether identityobj is already present or not
			Flag := true
			for _, obj := range e.CurDirectory {
				if identityobj.isEqual(&obj) {
					Flag = false
					break
				}
			}
			if Flag {
				// append the object if not already present
				e.CurDirectory = append(e.CurDirectory, identityobj)
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

func (e *Elastico) receiveNewNode(msg DecodeMsgType, epoch string) {
	var decodeMsg NewNodeMsg

	err := json.Unmarshal(msg.Data, &decodeMsg)
	FailOnError(err, "decode error in new node msg", true)
	// new node is added to the corresponding committee list if committee list has less than c members
	identityobj := decodeMsg.Identity
	// verify the PoW
	if e.verifyPoW(identityobj) {
		_, ok := e.CommitteeList[identityobj.CommitteeID]
		if ok == false {

			// Add the Identity in committee
			e.CommitteeList[identityobj.CommitteeID] = make([]IDENTITY, 0)
			e.CommitteeList[identityobj.CommitteeID] = append(e.CommitteeList[identityobj.CommitteeID], identityobj)

		} else if len(e.CommitteeList[identityobj.CommitteeID]) < c {
			// Add the Identity in committee
			Flag := true
			for _, obj := range e.CommitteeList[identityobj.CommitteeID] {
				if identityobj.isEqual(&obj) {
					Flag = false
					break
				}
			}
			if Flag {
				e.CommitteeList[identityobj.CommitteeID] = append(e.CommitteeList[identityobj.CommitteeID], identityobj)
				if len(e.CommitteeList[identityobj.CommitteeID]) == c {
					// check that if all committees are full
					e.checkCommitteeFull(epoch)
				}
			}
		}

	} else {
		logger.Error("PoW not valid in adding new node")
	}
}

func (e *Elastico) checkCommitteeFull(epoch string) {
	/*
		directory member checks whether the committees are full or not
	*/

	logger.Info("file:- elastico.go, func:- checkCommitteeFull()")
	commList := e.CommitteeList
	Flag := 0
	numOfCommittees := int64(math.Pow(2, float64(s)))
	// iterating over all committee ids
	for iden := int64(0); iden < numOfCommittees; iden++ {

		_, ok := commList[iden]
		if ok == false || len(commList[iden]) < c {

			logger.Warnf("committees not full  - bad miss id : %s", strconv.FormatInt(iden, 10))
			Flag = 1
			break
		}
	}
	if Flag == 0 {

		logger.Info("committees full  - good")
		if e.State == ElasticoStates["RunAsDirectory after-TxnReceived"] {

			// notify the final members
			e.notifyFinalCommittee(epoch)
			// multicast the txns and committee members to the nodes
			MulticastCommittee(commList, e.Identity, e.Txn, epoch)
			// change the state after multicast
			e.State = ElasticoStates["RunAsDirectory after-TxnMulticast"]
		}
	}
}

// MulticastCommittee :- each node getting Views of its committee members from directory members
func MulticastCommittee(commList map[int64][]IDENTITY, identityobj IDENTITY, txns map[int64][]*Message, epoch string) {

	logger.Info("file:- elastico.go, func:- MulticastCommittee()")
	// get the final committee members with the fixed committee id
	FinalCommitteeMembers := commList[finNum]
	for CommitteeID, commMembers := range commList {

		// find the Primary Identity, Take the first Identity
		// ToDo: fix this, many nodes can be Primary
		primaryID := commMembers[0]
		for _, memberID := range commMembers {

			// send the committee members , final committee members
			data := map[string]interface{}{"CommitteeMembers": commMembers, "FinalCommitteeMembers": FinalCommitteeMembers, "Identity": identityobj}

			// give txns only to the Primary node
			if memberID.isEqual(&primaryID) {
				data["Txns"] = txns[CommitteeID]
			} else {
				data["Txns"] = make([]*Message, 0)
			}
			fmt.Println("epoch : ", epoch)
			// construct the msg
			msg := map[string]interface{}{"data": data, "type": "committee members Views", "epoch": epoch}
			// send the committee member Views to nodes
			memberID.send(msg)
		}
	}
}

func (e *Elastico) notifyFinalCommittee(epoch string) {
	/*
		notify the members of the final committee that they are the final committee members
	*/
	logger.Info("file:- elastico.go, func:- notifyFinalCommittee()")
	finalCommList := e.CommitteeList[finNum]
	fmt.Println("len of final comm--", len(finalCommList))
	for _, finalMember := range finalCommList {
		data := map[string]interface{}{"Identity": e.Identity}
		// construct the msg
		msg := map[string]interface{}{"data": data, "type": "notify final member", "epoch": epoch}
		finalMember.send(msg)
	}
}

// CommitmentMsg - Commitment Message
type CommitmentMsg struct {
	Identity IDENTITY
	HashRi   string
}

func (e *Elastico) receiveHash(msg DecodeMsgType) {

	logger.Info("file:- elastico.go, func:- receiveHash()")
	// receiving H(Ri) by final committe members
	var decodeMsg CommitmentMsg
	err := json.Unmarshal(msg.Data, &decodeMsg)

	FailOnError(err, "fail to decode hash msg", true)

	identityobj := decodeMsg.Identity
	HashRi := decodeMsg.HashRi
	if e.verifyPoW(identityobj) {
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

func (e *Elastico) hexdigest(data string) string {
	/*
		Digest of data
	*/
	logger.Info("file:- elastico.go, func:- hexdigest of Elastico()")
	digest := sha256.New()
	digest.Write([]byte(data))

	hashVal := fmt.Sprintf("%x", digest.Sum(nil)) // convert to hexdigest
	return hashVal
}

func (e *Elastico) receiveRandomStringBroadcast(msg DecodeMsgType) {

	logger.Info("file:- elastico.go, func:-receiveRandomStringBroadcast()")
	var decodeMsg BroadcastRmsg

	err := json.Unmarshal(msg.Data, &decodeMsg)

	FailOnError(err, "fail to decode random string msg", true)

	identityobj := decodeMsg.Identity
	Ri := decodeMsg.Ri

	if e.verifyPoW(identityobj) {
		HashRi := e.hexdigest(Ri)

		if _, ok := e.NewRcommitmentSet[HashRi]; ok {

			e.NewsetOfRs[Ri] = true

			if len(e.NewsetOfRs) >= c/2+1 {
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
	Txnblock []*Message
	Sign     string
	Identity IDENTITY
}

func (e *Elastico) verifySignTxnList(TxnBlockSignature string, TxnBlock []*Message, PublicKey *rsa.PublicKey) bool {
	logger.Info("file:- elastico.go, func:- verifySignTxnList()")
	signed, err := base64.StdEncoding.DecodeString(TxnBlockSignature) // Decode the base64 encoded signature
	FailOnError(err, "Decode error of signature", true)
	// Sign the array of Transactions
	digest := sha256.New()
	for i := 0; i < len(TxnBlock); i++ {
		txnDigest := TxnBlock[i].hexdigest() // Get the transaction digest
		digest.Write([]byte(txnDigest))
	}
	err = rsa.VerifyPKCS1v15(PublicKey, crypto.SHA256, digest.Sum(nil), signed) // verify the sign of digest of Txn List
	if err != nil {
		return false
	}
	return true
}

func (e *Elastico) receiveIntraCommitteeBlock(msg DecodeMsgType) {

	logger.Info("file:- elastico.go, func:- receiveIntraCommitteeBlock()")

	// final committee member receives the final set of txns along with the signature from the node
	var decodeMsg IntraBlockMsg
	err := json.Unmarshal(msg.Data, &decodeMsg)
	FailOnError(err, "error in unmarshal intra committee block", true)

	identityobj := decodeMsg.Identity

	if e.verifyPoW(identityobj) {
		signature := decodeMsg.Sign
		TxnBlock := decodeMsg.Txnblock
		// verify the signatures
		PK := identityobj.PK
		if e.verifySignTxnList(signature, TxnBlock, &PK) {
			if _, ok := e.CommitteeConsensusData[identityobj.CommitteeID]; ok == false {

				e.CommitteeConsensusData[identityobj.CommitteeID] = make(map[string][]string)
				e.CommitteeConsensusDataTxns[identityobj.CommitteeID] = make(map[string][]*Message)
			}
			TxnBlockDigest := txnHexdigest(TxnBlock)
			if _, okk := e.CommitteeConsensusData[identityobj.CommitteeID][TxnBlockDigest]; okk == false {
				e.CommitteeConsensusData[identityobj.CommitteeID][TxnBlockDigest] = make([]string, 0)
				// store the txns for this digest
				e.CommitteeConsensusDataTxns[identityobj.CommitteeID][TxnBlockDigest] = TxnBlock
			}

			// add signatures for the Txn block
			e.CommitteeConsensusData[identityobj.CommitteeID][TxnBlockDigest] = append(e.CommitteeConsensusData[identityobj.CommitteeID][TxnBlockDigest], signature)

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

func intersection(set1 map[string]bool, set2 []string) map[string]bool {
	// This function takes intersection of two maps
	// intersectSet := make(map[string]bool)
	logger.Info("file:- elastico.go, func:- intersection()")
	for _, s1 := range set2 {
		// _, ok := set1[s1]
		// if ok == true {
		set1[s1] = true
		// }
	}

	return set1
}

func (e *Elastico) receiveConsistency(msg DecodeMsgType) {
	logger.Info("file:- elastico.go, func:- receiveConsistency()")
	// receive consistency msgs
	var decodeMsg IntraConsistencyMsg
	err := json.Unmarshal(msg.Data, &decodeMsg)
	FailOnError(err, "error in unmarshal intra committee block", true)

	identityobj := decodeMsg.Identity

	if e.verifyPoW(identityobj) {
		Commitments := decodeMsg.Commitments
		if len(e.EpochcommitmentSet) == 0 {
			for _, commitment := range Commitments {
				e.EpochcommitmentSet[commitment] = true
			}
		} else {
			e.EpochcommitmentSet = intersection(e.EpochcommitmentSet, Commitments)
		}
		// logger.Info("received Int. Consistency Commitments! ", len(e.EpochcommitmentSet), " port :", e.Port, " Commitments : ", Commitments)
	} else {
		logger.Error("pow invalid for intra committee block")
	}
}

func (e *Elastico) receive(msg DecodeMsgType, epoch string) {
	/*
		method to recieve messages for a node as per the type of a msg
	*/
	// new node is added in directory committee if not yet formed
	if msg.Type == "directoryMember" {
		e.receiveDirectoryMember(msg)

	} else if msg.Type == "newNode" && e.IsDirectory {
		e.receiveNewNode(msg, epoch)

	} else if msg.Type == "committee members Views" && e.IsDirectory == false {
		e.receiveViews(msg)

	} else if msg.Type == "hash" && e.isFinalMember() {
		e.receiveHash(msg)

	} else if msg.Type == "RandomStringBroadcast" {
		e.receiveRandomStringBroadcast(msg)

	} else if msg.Type == "pre-prepare" || msg.Type == "prepare" || msg.Type == "commit" {

		e.pbftProcessMessage(msg)
	} else if msg.Type == "intraCommitteeBlock" && e.isFinalMember() {

		e.receiveIntraCommitteeBlock(msg)
	} else if msg.Type == "InteractiveConsistency" && e.isFinalMember() {

		e.receiveConsistency(msg)

	} else if msg.Type == "notify final member" {
		logger.Infof("notifying final member %s", e.Port)
		var decodeMsg NotifyFinalMsg
		err := json.Unmarshal(msg.Data, &decodeMsg)
		FailOnError(err, "error in decoding final member msg", true)
		identityobj := decodeMsg.Identity
		if e.verifyPoW(identityobj) && e.CommitteeID == finNum {
			e.IsFinal = true
		}
	} else if msg.Type == "Finalpre-prepare" || msg.Type == "Finalprepare" || msg.Type == "Finalcommit" {
		e.FinalpbftProcessMessage(msg)
	} else if msg.Type == "FinalBlock" {

		e.receiveFinalTxnBlock(msg)

	} else if msg.Type == "reset-all" {
		var decodeMsg ResetMsg
		err := json.Unmarshal(msg.Data, &decodeMsg)
		FailOnError(err, "fail to decode reset msg", true)
		if e.verifyPoW(decodeMsg.Identity) {
			// reset the elastico node
			e.reset()
		}

	}
}

// FinalBlockMsg - final block msg
type FinalBlockMsg struct {
	CommitSet      []string
	Signature      string
	Identity       IDENTITY
	FinalBlock     []*Message
	FinalBlockSign string
}

func (is *IdentityAndSign) isEqual(data IdentityAndSign) bool {
	/*
		compare two objects
	*/
	logger.Info("file:- elastico.go, func:- isEqual() of iden and sign")
	return is.sign == data.sign && is.identityobj.isEqual(&data.identityobj)
}

func (e *Elastico) receiveFinalTxnBlock(msg DecodeMsgType) {
	logger.Info("file:- elastico.go, func:- receiveFinalTxnBlock()")
	var decodeMsg FinalBlockMsg
	err := json.Unmarshal(msg.Data, &decodeMsg)
	FailOnError(err, "fail in decoding the final block msg", true)

	identityobj := decodeMsg.Identity
	// verify the PoW of the sender
	if e.verifyPoW(identityobj) {

		sign := decodeMsg.Signature
		receivedCommitments := decodeMsg.CommitSet
		finalTxnBlock := decodeMsg.FinalBlock

		finalTxnBlockSignature := decodeMsg.FinalBlockSign

		// verify the signatures
		receivedCommitmentDigest := e.digestCommitments(receivedCommitments)
		PK := identityobj.PK
		if e.verifySign(sign, receivedCommitmentDigest, &PK) && e.verifySignTxnList(finalTxnBlockSignature, finalTxnBlock, &PK) {

			// list init for final Txn block
			finaltxnBlockDigest := txnHexdigest(finalTxnBlock)
			if _, ok := e.FinalBlockbyFinalCommittee[finaltxnBlockDigest]; ok == false {
				e.FinalBlockbyFinalCommittee[finaltxnBlockDigest] = make([]IdentityAndSign, 0)
				e.FinalBlockbyFinalCommitteeTxns[finaltxnBlockDigest] = finalTxnBlock
			}

			// creating the object that contains the Identity and signature of the final member
			identityAndSign := IdentityAndSign{finalTxnBlockSignature, identityobj}

			// check whether this combination of Identity and sign already exists or not
			Flag := true
			for _, idSignObj := range e.FinalBlockbyFinalCommittee[finaltxnBlockDigest] {

				if idSignObj.isEqual(identityAndSign) {
					// it exists
					Flag = false
					break
				}
			}
			if Flag {
				// appending the Identity and sign of final member
				e.FinalBlockbyFinalCommittee[finaltxnBlockDigest] = append(e.FinalBlockbyFinalCommittee[finaltxnBlockDigest], identityAndSign)
			}

			// block is signed by sufficient final members and when the final block has not been sent to the client yet
			if len(e.FinalBlockbyFinalCommittee[finaltxnBlockDigest]) >= c/2+1 && e.State != ElasticoStates["FinalBlockSentToClient"] {
				// for final members, their state is updated only when they have also sent the finalblock to ntw
				if e.isFinalMember() {
					finalBlockSent := e.FinalBlock.Sent
					if finalBlockSent {

						e.State = ElasticoStates["FinalBlockReceived"]
					}

				} else {

					e.State = ElasticoStates["FinalBlockReceived"]
				}

			}
			// union of Commitments
			e.unionSet(receivedCommitments)

		} else {

			logger.Error("Signature invalid in final block received")
		}
	} else {
		logger.Error("PoW not valid when final member send the block")
	}
}

func (e *Elastico) unionSet(receivedSet []string) {
	logger.Info("file:- elastico.go, func:- unionSet()")
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
		e.processFinalprePrepareMsg(msg)

	} else if msg.Type == "Finalprepare" {
		e.processFinalprepareMsg(msg)

	} else if msg.Type == "Finalcommit" {
		e.processFinalcommitMsg(msg)
	}
}

func (e *Elastico) processPrePrepareMsg(msg DecodeMsgType) {
	/*
		Process Pre-Prepare msg
	*/
	logger.Info("file:- elastico.go, func:- processPrePrepareMsg()")
	var decodeMsg PrePrepareMsg

	err := json.Unmarshal(msg.Data, &decodeMsg)
	FailOnError(err, "fail to decode pre-prepare msg", true)

	// verify the pre-prepare message

	verified := e.verifyPrePrepare(decodeMsg)
	if verified {
		// Log the pre-prepare msgs!
		logger.Info("pre-prepared verified by port--", e.Port)
		e.logPrePrepareMsg(decodeMsg)

	} else {
		logger.Error("error in verification of processPrePrepareMsg")
	}
}

func (e *Elastico) verifySign(signature string, digest []byte, PublicKey *rsa.PublicKey) bool {
	/*
		verify whether signature is valid or not
	*/
	logger.Info("file:- elastico.go, func:- verifySign()")
	signed, err := base64.StdEncoding.DecodeString(signature) // Decode the base64 encoded signature
	FailOnError(err, "Decode error of signature", true)
	err = rsa.VerifyPKCS1v15(PublicKey, crypto.SHA256, digest, signed) // verify the sign of digest
	if err != nil {
		return false
	}
	return true
}

func (e *Elastico) verifyPrePrepare(msg PrePrepareMsg) bool {
	/*
		Verify pre-prepare msgs
	*/
	logger.Info("file:- elastico.go, func:- verifyPrePrepare()")
	identityobj := msg.Identity
	prePreparedData := msg.PrePrepareData
	txnBlockList := msg.Message
	// verify Pow
	if e.verifyPoW(identityobj) == false {

		logger.Error("wrong pow in  verify pre-prepare")
		return false
	}
	// verify signatures of the received msg
	sign := msg.Sign
	prePreparedDataDigest := e.digestPrePrepareMsg(prePreparedData)
	PK := identityobj.PK
	if e.verifySign(sign, prePreparedDataDigest, &PK) == false {

		logger.Error("wrong sign in  verify pre-prepare")
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

func (e *Elastico) processFinalprePrepareMsg(msg DecodeMsgType) {
	/*
		Process Final Pre-Prepare msg
	*/
	logger.Info("file:- elastico.go, func:- processFinalprePrepareMsg()")
	var decodeMsg PrePrepareMsg

	err := json.Unmarshal(msg.Data, &decodeMsg)
	FailOnError(err, "fail to decode final pre-prepare msg", true)

	logger.Info("final pre-prepare msg of port", e.Port, "msg--", decodeMsg)
	// verify the Final pre-prepare message
	verified := e.verifyFinalPrePrepare(decodeMsg)
	if verified {
		logger.Info("final pre-prepare verified")
		// Log the final pre-prepare msgs!
		e.logFinalPrePrepareMsg(decodeMsg)

	} else {
		logger.Error("error in verification of Final processPrePrepareMsg")
	}
}

func (e *Elastico) verifyFinalPrePrepare(msg PrePrepareMsg) bool {
	/*
		Verify final pre-prepare msgs
	*/
	logger.Info("file:- elastico.go, func:- verifyFinalPrePrepare()")
	identityobj := msg.Identity
	prePreparedData := msg.PrePrepareData
	txnBlockList := msg.Message

	// verify Pow
	if e.verifyPoW(identityobj) == false {

		logger.Warn("wrong pow in  verify final pre-prepare")
		return false
	}
	// verify signatures of the received msg
	sign := msg.Sign
	prePreparedDataDigest := e.digestPrePrepareMsg(prePreparedData)
	PK := identityobj.PK
	if e.verifySign(sign, prePreparedDataDigest, &PK) == false {

		logger.Warn("wrong sign in  verify final pre-prepare")
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

func (e *Elastico) verifyCommit(msg CommitMsg) bool {
	/*
		verify commit msgs
	*/
	logger.Info("file:- elastico.go, func:- verifyCommit()")
	// verify Pow
	identityobj := msg.Identity
	if !e.verifyPoW(identityobj) {
		return false
	}
	// verify signatures of the received msg

	sign := msg.Sign
	commitData := msg.CommitData
	digestCommitData := e.digestCommitMsg(commitData)
	PK := identityobj.PK
	if !e.verifySign(sign, digestCommitData, &PK) {
		return false
	}

	// check the view is same or not
	ViewID := commitData.ViewID
	if ViewID != e.ViewID {
		return false
	}
	return true
}

func (e *Elastico) logCommitMsg(msg CommitMsg) {
	/*
	 log the commit msg
	*/
	logger.Info("file:- elastico.go, func:- logCommitMsg()")
	identityobj := msg.Identity
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
	msgDetails := CommitMsgData{Digest: commitDataDigest, Identity: identityobj}
	// append msg
	commitMsgLogSocket := commitMsgLogSeq[socketID]
	commitMsgLogSocket = append(commitMsgLogSocket, msgDetails)
	e.CommitMsgLog[ViewID][seqnum][socketID] = commitMsgLogSocket
}

func (e *Elastico) processCommitMsg(msg DecodeMsgType) {
	/*
		process the commit msg
	*/
	// verify the commit message
	logger.Info("file:- elastico.go, func:- processCommitMsg()")
	var decodeMsg CommitMsg

	err := json.Unmarshal(msg.Data, &decodeMsg)
	FailOnError(err, "fail to decode commit msg", true)

	logger.Info("commit msg in--", e.Port, "msg -- ", decodeMsg)
	verified := e.verifyCommit(decodeMsg)
	if verified {
		logger.Info("commit verified")
		// Log the commit msgs!
		e.logCommitMsg(decodeMsg)
	}
}

func (e *Elastico) processFinalcommitMsg(msg DecodeMsgType) {
	/*
		process the final commit msg
	*/
	logger.Info("file:- elastico.go, func:- processFinalcommitMsg()")
	// verify the commit message
	var decodeMsg CommitMsg

	err := json.Unmarshal(msg.Data, &decodeMsg)
	FailOnError(err, "fail to decode final commit msg", true)

	logger.Info("final commit msg in port--", e.Port, "with msg--", decodeMsg)
	verified := e.verifyCommit(decodeMsg)
	if verified {
		fmt.Println("final commit verified")
		// Log the commit msgs!
		e.logFinalCommitMsg(decodeMsg)
	}
}

func (e *Elastico) logFinalCommitMsg(msg CommitMsg) {
	/*
		log the final commit msg
	*/
	logger.Info("file:- elastico.go, func:- logFinalCommitMsg()")
	identityobj := msg.Identity
	commitContents := msg.CommitData
	ViewID := commitContents.ViewID
	seqnum := commitContents.Seq

	socketID := identityobj.IP + ":" + identityobj.Port
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
	msgDetails := CommitMsgData{Digest: commitDataDigest, Identity: identityobj}
	// append msg
	commitMsgLogSocket := commitMsgLogSeq[socketID]
	commitMsgLogSocket = append(commitMsgLogSocket, msgDetails)
	e.FinalcommitMsgLog[ViewID][seqnum][socketID] = commitMsgLogSocket

}

func (e *Elastico) pbftProcessMessage(msg DecodeMsgType) {
	/*
		Process the messages related to Pbft!
	*/
	logger.Info("file:- elastico.go, func:- pbftProcessMessage()")
	if msg.Type == "pre-prepare" {

		e.processPrePrepareMsg(msg)

	} else if msg.Type == "prepare" {

		e.processPrepareMsg(msg)

	} else if msg.Type == "commit" {
		e.processCommitMsg(msg)
	}
}

func (e *Elastico) processPrepareMsg(msg DecodeMsgType) {
	/*
		process prepare msg
	*/
	logger.Info("file:- elastico.go, func:- processPrepareMsg()")
	// verify the prepare message
	var decodeMsg PrepareMsg

	err := json.Unmarshal(msg.Data, &decodeMsg)
	FailOnError(err, "fail to decode prepare msg", true)
	logger.Infof("prepare msg in-- %s msg---", e.Port)

	verified := e.verifyPrepare(decodeMsg)
	if verified {
		logger.Info("prepare verified")
		// Log the prepare msgs!
		e.logPrepareMsg(decodeMsg)
	}
}

func (e *Elastico) logPrepareMsg(msg PrepareMsg) {
	/*
		log the prepare msg
	*/
	logger.Info("file:- elastico.go, func:- logPrepareMsg()")
	identityobj := msg.Identity
	prepareData := msg.PrepareData
	ViewID := prepareData.ViewID
	seqnum := prepareData.Seq

	socketID := identityobj.IP + ":" + identityobj.Port
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
	msgDetails := PrepareMsgData{Digest: prepareDataDigest, Identity: identityobj}
	// append msg to prepare msg log
	prepareMsgLogSocket := prepareMsgLogSeq[socketID]
	prepareMsgLogSocket = append(prepareMsgLogSocket, msgDetails)
	e.PrepareMsgLog[ViewID][seqnum][socketID] = prepareMsgLogSocket

}

func (e *Elastico) verifyPrepare(msg PrepareMsg) bool {
	/*
	 Verify prepare msgs
	*/
	logger.Info("file:- elastico.go, func:- verifyPrepare()")
	// verify Pow
	identityobj := msg.Identity
	if e.verifyPoW(identityobj) == false {

		logger.Error("wrong pow in verify prepares")
		return false
	}

	// verify signatures of the received msg
	sign := msg.Sign
	prepareData := msg.PrepareData
	PrepareContentsDigest := e.digestPrepareMsg(prepareData)
	PK := identityobj.PK
	if e.verifySign(sign, PrepareContentsDigest, &PK) == false {

		logger.Error("wrong sign in verify prepares")
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

func (e *Elastico) verifyFinalPrepare(msg PrepareMsg) bool {
	/*
		Verify final prepare msgs
	*/
	logger.Info("file:- elastico.go, func:- verifyFinalPrepare()")
	// verify Pow
	identityobj := msg.Identity
	if e.verifyPoW(identityobj) == false {

		logger.Warn("wrong pow in verify final prepares")
		return false
	}
	// verify signatures of the received msg
	sign := msg.Sign
	prepareData := msg.PrepareData
	PrepareContentsDigest := e.digestPrepareMsg(prepareData)

	PK := identityobj.PK

	if e.verifySign(sign, PrepareContentsDigest, &PK) == false {

		logger.Warn("wrong sign in verify final prepares")
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

func (e *Elastico) processFinalprepareMsg(msg DecodeMsgType) {
	/*
		process final prepare msg
	*/
	logger.Info("file:- elastico.go, func:- processFinalprepareMsg()")
	var decodeMsg PrepareMsg
	err := json.Unmarshal(msg.Data, &decodeMsg)
	FailOnError(err, "fail to decode final prepare msg", true)
	// verify the prepare message

	logger.Info("final prepare msg of port--", e.Port, "with msg--", decodeMsg)
	verified := e.verifyFinalPrepare(decodeMsg)
	if verified {
		fmt.Println("FINAL PREPARE VERIFIED with port--", e.Port)
		// Log the prepare msgs!
		e.logFinalPrepareMsg(decodeMsg)
	}
}

func (e *Elastico) logFinalPrepareMsg(msg PrepareMsg) {
	/*
		log the prepare msg
	*/
	logger.Info("file:- elastico.go, func:- logFinalPrepareMsg()")
	identityobj := msg.Identity
	prepareData := msg.PrepareData
	ViewID := prepareData.ViewID
	seqnum := prepareData.Seq

	socketID := identityobj.IP + ":" + identityobj.Port

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
	msgDetails := PrepareMsgData{Digest: prepareDataDigest, Identity: identityobj}
	// append msg to prepare msg log
	prepareMsgLogSocket := prepareMsgLogSeq[socketID]
	prepareMsgLogSocket = append(prepareMsgLogSocket, msgDetails)
	e.FinalPrepareMsgLog[ViewID][seqnum][socketID] = prepareMsgLogSocket
}

func (e *Elastico) unionViews(nodeData, incomingData []IDENTITY) []IDENTITY {
	/*
		nodeData and incomingData are the set of identities
		Adding those identities of incomingData to nodeData that are not present in nodeData
	*/
	logger.Info("file:- elastico.go, func:- unionViews()")
	for _, data := range incomingData {

		Flag := false
		for _, nodeID := range nodeData {

			// data is present already in nodeData
			if nodeID.isEqual(&data) {
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
	Txns                  []*Message
}

func (e *Elastico) receiveViews(msg DecodeMsgType) {
	// logger.Info("file:- elastico.go, func:- receiveViews()")
	var decodeMsg ViewsMsg

	err := json.Unmarshal(msg.Data, &decodeMsg)
	// fmt.Println("Views msg---", decodeMsg)
	FailOnError(err, "fail to decode Views msg", true)

	identityobj := decodeMsg.Identity

	if e.verifyPoW(identityobj) {

		if _, ok := e.Views[identityobj.IP+identityobj.Port]; ok == false {

			// union of committe members Views
			e.Views[identityobj.IP+identityobj.Port] = true

			commMembers := decodeMsg.CommitteeMembers
			finalMembers := decodeMsg.FinalCommitteeMembers
			Txns := decodeMsg.Txns

			// update the Txn block
			// ToDo: txnblock should be ordered, not set
			if len(Txns) > 0 {

				e.TxnBlock = e.unionTxns(e.TxnBlock, Txns)
				logger.Infof("I am Primary %s", e.IP)
				e.Primary = true
			}

			// ToDo: verify this union thing
			// union of committee members wrt directory member
			e.CommitteeMembers = e.unionViews(e.CommitteeMembers, commMembers)
			// union of final committee members wrt directory member
			e.FinalCommitteeMembers = e.unionViews(e.FinalCommitteeMembers, finalMembers)
			// received the members
			if e.State == ElasticoStates["Formed Committee"] && len(e.Views) >= c/2+1 {

				e.State = ElasticoStates["Receiving Committee Members"]
			}
		}
	}
}
