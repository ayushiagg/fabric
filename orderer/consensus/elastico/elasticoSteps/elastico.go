package elasticoSteps

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	random "math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/orderer/consensus/elastico/connection"
	"github.com/streadway/amqp"
)

// D - difficulty level , leading bits of PoW must have D 0's (keep w.r.t to hex)
var D = 4

// c - size of committee
var c = 4

// r - number of bits in random string
var r int64 = 8

// s - where 2^s is the number of committees
var s = 2

var logger = flogging.MustGetLogger("orderer.consensus.elastico.elasticoSteps")

// ElasticoStates - states reperesenting the running state of the node
var ElasticoStates = map[string]int{"NONE": 0, "PoW Computed": 1, "Formed Identity": 2, "Formed Committee": 3, "RunAsDirectory": 4, "RunAsDirectory after-TxnReceived": 5, "RunAsDirectory after-TxnMulticast": 6, "Receiving Committee Members": 7, "PBFT_NONE": 8, "PBFT_PRE_PREPARE": 9, "PBFT_PRE_PREPARE_SENT": 10, "PBFT_PREPARE_SENT": 11, "PBFT_PREPARED": 12, "PBFT_COMMITTED": 13, "PBFT_COMMIT_SENT": 14, "Intra Consensus Result Sent to Final": 15, "Merged Consensus Data": 16, "FinalPBFT_NONE": 17, "FinalPBFT_PRE_PREPARE": 18, "FinalPBFT_PRE_PREPARE_SENT": 19, "FinalPBFT_PREPARE_SENT": 20, "FinalPBFT_PREPARED": 21, "FinalPBFT_COMMIT_SENT": 22, "FinalPBFT_COMMITTED": 23, "PBFT Finished-FinalCommittee": 24, "CommitmentSentToFinal": 25, "InteractiveConsistencyStarted": 33, "InteractiveConsistencyAchieved": 26, "FinalBlockSent": 27, "FinalBlockReceived": 28, "BroadcastedR": 29, "ReceivedR": 30, "FinalBlockSentToClient": 31, "LedgerUpdated": 32}

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

// Transaction :- structure for transaction
type Transaction struct {
	Sender   string
	Receiver string
	Amount   *big.Int // RandomGen returns *big.Int
	// ToDo: include timestamp or not
}

// IdentityAndSign :- for signature and its Identity which can be used for verification
type IdentityAndSign struct {
	sign        string
	identityobj IDENTITY
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
	txnList                       []Transaction
	listSignaturesAndIdentityobjs []IdentityAndSign
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
		epoch_randomness - r-bit random string generated at the end of previous epoch
		Ri - r-bit random string
		commitments - set of H(Ri) received by final committee node members and H(Ri) is sent by the final committee node only
		txn_block - block of txns that the committee will agree on(intra committee consensus block)
		set_of_Rs - set of Ris obtained from the final committee of previous epoch
		newset_of_Rs - In the present epoch, set of Ris obtained from the final committee
		CommitteeConsensusData - a dictionary of committee ids that contains a dictionary of the txn block and the signatures
		finalBlockbyFinalCommittee - a dictionary of txn block and the signatures by the final committee members
		state - state in which a node is running
		mergedBlock - list of txns of different committees after their intra committee consensus
		finalBlock - agreed list of txns after pbft run by final committee
		RcommitmentSet - set of H(Ri)s received from the final committee after the consistency protocol [previous epoch values]
		newRcommitmentSet - For the present it contains the set of H(Ri)s received from the final committee after the consistency protocol
		finalCommitteeMembers - members of the final committee received from the directory committee
		txn- transactions stored by the directory members
		response - final block to be received by the client
		flag- to denote a bad or good node
		views - stores the ports of processes from which committee member views have been received
		primary- boolean to denote the primary node in the committee for PBFT run
		viewID - view number of the pbft
		prePrepareMsgLog - log of pre-prepare msgs received during PBFT
		prepareMsgLog - log of prepare msgs received during PBFT
		commitMsgLog - log of commit msgs received during PBFT
		preparedData - data after prepared state
		committedData - data after committed state
		Finalpre_prepareMsgLog - log of pre-prepare msgs received during PBFT run by final committee
		FinalprepareMsgLog - log of prepare msgs received during PBFT run by final committee
		FinalcommitMsgLog - log of commit msgs received during PBFT run by final committee
		FinalpreparedData - data after prepared state in final pbft run
		FinalcommittedData - data after committed state in final pbft run
		faulty - Flag denotes whether this node is faulty or not
	*/
	connection   *amqp.Connection
	IP           string
	Port         string
	key          *rsa.PrivateKey
	PoW          PoWmsg
	curDirectory []IDENTITY
	Identity     IDENTITY
	CommitteeID  int64
	// only when this node is the member of directory committee
	committeeList map[int64][]IDENTITY
	// only when this node is not the member of directory committee
	committeeMembers []IDENTITY
	isDirectory      bool
	isFinal          bool
	EpochRandomness  string
	Ri               string
	// only when this node is the member of final committee
	commitments                    map[string]bool
	txnBlock                       []Transaction
	setOfRs                        map[string]bool
	newsetOfRs                     map[string]bool
	CommitteeConsensusData         map[int64]map[string][]string
	CommitteeConsensusDataTxns     map[int64]map[string][]Transaction
	finalBlockbyFinalCommittee     map[string][]IdentityAndSign
	finalBlockbyFinalCommitteeTxns map[string][]Transaction
	state                          int
	mergedBlock                    []Transaction
	finalBlock                     FinalBlockData
	RcommitmentSet                 map[string]bool
	newRcommitmentSet              map[string]bool
	finalCommitteeMembers          []IDENTITY
	// only when this is the member of the directory committee
	txn                   map[int64][]Transaction
	response              []FinalCommittedBlock
	flag                  bool
	views                 map[int]bool
	primary               bool
	viewID                int
	faulty                bool
	prePrepareMsgLog      map[string]PrePrepareMsg
	prepareMsgLog         map[int]map[int]map[string][]PrepareMsgData
	commitMsgLog          map[int]map[int]map[string][]CommitMsgData
	preparedData          map[int]map[int][]Transaction
	committedData         map[int]map[int][]Transaction
	FinalPrePrepareMsgLog map[string]PrePrepareMsg
	FinalPrepareMsgLog    map[int]map[int]map[string][]PrepareMsgData
	FinalcommitMsgLog     map[int]map[int]map[string][]CommitMsgData
	FinalpreparedData     map[int]map[int][]Transaction
	FinalcommittedData    map[int]map[int][]Transaction
	EpochcommitmentSet    map[string]bool
}

func (e *Elastico) getIP() {
	/*
		for each node(processor) , get IP addr
	*/
	e.IP = os.Getenv("ORDERER_HOST")

}

// RandomGen :-
func RandomGen(r int64) *big.Int {
	/*
		generate a random integer
	*/
	// n is the base, e is the exponent, creating big.Int variables
	var num, e = big.NewInt(2), big.NewInt(r)
	// taking the exponent n to the power e and nil modulo, and storing the result in n
	num.Exp(num, e, nil)
	// generates the random num in the range[0,n)
	// here Reader is a global, shared instance of a cryptographically secure random number generator.
	randomNum, err := rand.Int(rand.Reader, num)

	connection.FailOnError(err, "random number generation", true)
	return randomNum
}

func (e *Elastico) getPort() {
	/*
		get Port number for the process
	*/
	// acquire the lock
	e.Port = os.Getenv("ORDERER_GENERAL_LISTENPORT")
}

func (e *Elastico) getKey() {
	/*
		for each node, it will set key as public pvt key pair
	*/
	var err error
	// generate the public-pvt key pair
	e.key, err = rsa.GenerateKey(rand.Reader, 2048)
	connection.FailOnError(err, "key generation", true)
}

func (e *Elastico) initER() {
	/*
		initialise r-bit epoch random string
	*/

	randomnum := RandomGen(r)
	// set r-bit binary string to epoch randomness
	e.EpochRandomness = fmt.Sprintf("%0"+strconv.FormatInt(r, 10)+"b\n", randomnum)
}

// ElasticoInit :- initialise of data members
func (e *Elastico) ElasticoInit() {
	var err error
	// create rabbit mq connection
	e.connection, err = amqp.Dial("amqp://guest:guest@localhost:5672/")
	// log if connection fails
	connection.FailOnError(err, "Failed to connect to RabbitMQ", true)
	// set IP
	e.getIP()
	e.getPort()
	// set RSA
	e.getKey()
	// Initialize PoW!
	e.PoW = PoWmsg{}
	e.PoW.Hash = ""
	e.PoW.SetOfRs = make([]string, 0)
	e.PoW.Nonce = 0

	e.curDirectory = make([]IDENTITY, 0)

	e.committeeList = make(map[int64][]IDENTITY)

	e.committeeMembers = make([]IDENTITY, 0)

	e.CommitteeID = -1
	// for setting EpochRandomness
	e.initER()

	e.commitments = make(map[string]bool)

	e.txnBlock = make([]Transaction, 0)

	e.setOfRs = make(map[string]bool)

	e.newsetOfRs = make(map[string]bool)

	e.CommitteeConsensusData = make(map[int64]map[string][]string)

	e.CommitteeConsensusDataTxns = make(map[int64]map[string][]Transaction)

	e.finalBlockbyFinalCommittee = make(map[string][]IdentityAndSign)

	e.finalBlockbyFinalCommitteeTxns = make(map[string][]Transaction)

	e.state = ElasticoStates["NONE"]

	e.mergedBlock = make([]Transaction, 0)

	e.finalBlock = FinalBlockData{}
	e.finalBlock.Sent = false
	e.finalBlock.Txns = make([]Transaction, 0)

	e.RcommitmentSet = make(map[string]bool)
	e.newRcommitmentSet = make(map[string]bool)
	e.finalCommitteeMembers = make([]IDENTITY, 0)

	e.txn = make(map[int64][]Transaction)
	e.response = make([]FinalCommittedBlock, 0)
	e.flag = true
	e.views = make(map[int]bool)
	e.primary = false
	e.viewID = 0
	e.faulty = false

	e.prePrepareMsgLog = make(map[string]PrePrepareMsg)
	e.prepareMsgLog = make(map[int]map[int]map[string][]PrepareMsgData)
	e.commitMsgLog = make(map[int]map[int]map[string][]CommitMsgData)
	e.preparedData = make(map[int]map[int][]Transaction)
	e.committedData = make(map[int]map[int][]Transaction)
	e.FinalPrePrepareMsgLog = make(map[string]PrePrepareMsg)
	e.FinalPrepareMsgLog = make(map[int]map[int]map[string][]PrepareMsgData)
	e.FinalcommitMsgLog = make(map[int]map[int]map[string][]CommitMsgData)
	e.FinalpreparedData = make(map[int]map[int][]Transaction)
	e.FinalcommittedData = make(map[int]map[int][]Transaction)
	e.EpochcommitmentSet = make(map[string]bool)
}

type msgType struct {
	Type string
	Data json.RawMessage
}

func (e *Elastico) executePoW() {
	/*
		execute PoW
	*/
	if e.flag {
		// compute Pow for good node
		e.computePoW()
	} else {
		// compute Pow for bad node
		// e.computeFakePoW()
	}
}

func (e *Elastico) computePoW() {
	/*
		returns hash which satisfies the difficulty challenge(D) : PoW["Hash"]
	*/
	zeroString := ""
	for i := 0; i < D; i++ {
		zeroString += "0"
	}
	if e.state == ElasticoStates["NONE"] {
		nonce := e.PoW.Nonce
		PK := e.key.PublicKey // public key
		IP := e.IP + e.Port
		// If it is the first epoch , randomsetR will be an empty set .
		// otherwise randomsetR will be any c/2 + 1 random strings Ri that node receives from the previous epoch
		randomsetR := make([]string, 0)
		if len(e.setOfRs) > 0 {
			e.EpochRandomness, randomsetR = e.xorR()
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
			e.state = ElasticoStates["PoW Computed"]
		} else {
			// try for other nonce
			nonce++
			e.PoW.Nonce = nonce
		}
	}
}

func sample(A []string, x int) []string {
	// randomly sample x values from list of strings A
	random.Seed(time.Now().UnixNano())
	randomize := random.Perm(len(A)) // get the random permutation of indices of A

	sampleslice := make([]string, 0)

	for _, v := range randomize[:x] {
		sampleslice = append(sampleslice, A[v])
	}
	return sampleslice
}

func xorbinary(A []string) int64 {
	// returns xor of the binary strings of A
	var xorVal int64
	for i := range A {
		intval, _ := strconv.ParseInt(A[i], 2, 0)
		xorVal = xorVal ^ intval
	}
	return xorVal
}

func (e *Elastico) xorR() (string, []string) {
	// find xor of any random c/2 + 1 r-bit strings to set the epoch randomness
	listOfRs := make([]string, 0)
	for R := range e.setOfRs {
		listOfRs = append(listOfRs, R)
	}
	randomset := sample(listOfRs, c/2+1) //get random c/2 + 1 strings from list of Rs
	xorVal := xorbinary(randomset)
	xorString := fmt.Sprintf("%0"+strconv.FormatInt(r, 10)+"b\n", xorVal) //converting xor value to r-bit string
	return xorString, randomset
}

func (e *Elastico) getCommitteeid() {
	/*
		sets last s-bit of PoW["Hash"] as Identity : CommitteeID
	*/
	// PoW := e.PoW["Hash"].(string)
	PoW := e.PoW.Hash
	bindigest := ""

	for i := 0; i < len(PoW); i++ {
		intVal, err := strconv.ParseInt(string(PoW[i]), 16, 0) // converts hex string to integer
		connection.FailOnError(err, "string to int conversion error", true)
		bindigest += fmt.Sprintf("%04b", intVal) // converts intVal to 4 bit binary value
	}
	// take last s bits of the binary digest
	Identity := bindigest[len(bindigest)-s:]
	iden, err := strconv.ParseInt(Identity, 2, 0) // converts binary string to integer
	connection.FailOnError(err, "binary to int conversion error", true)
	// logger.Info("Committe id : ", iden)
	e.CommitteeID = iden
}

func (e *Elastico) formIdentity() {
	/*
		Identity formation for a node
		Identity consists of public key, ip, committee id, PoW, nonce, epoch randomness
	*/
	if e.state == ElasticoStates["PoW Computed"] {

		PK := e.key.PublicKey

		// set the committee id acc to PoW solution
		e.getCommitteeid()

		e.Identity = IDENTITY{IP: e.IP, PK: PK, CommitteeID: e.CommitteeID, PoW: e.PoW, EpochRandomness: e.EpochRandomness, Port: e.Port}
		// changed the state after Identity formation
		e.state = ElasticoStates["Formed Identity"]
	}
}

func marshalData(msg map[string]interface{}) []byte {

	body, err := json.Marshal(msg)
	// fmt.Println("marshall data", body)
	connection.FailOnError(err, "error in marshal", true)
	return body
}

// BroadcastToNetwork - Broadcast data to the whole ntw
func BroadcastToNetwork(exchangeName string, msg map[string]interface{}) {

	conn := connection.GetConnection()
	defer conn.Close() // close the connection

	channel := connection.GetChannel(conn)
	defer channel.Close() // close the channel

	body := marshalData(msg)

	err := channel.Publish(
		exchangeName, // exchange
		"",           // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        body,
		})

	connection.FailOnError(err, "Failed to publish a message", true)

}

func (e *Elastico) formCommittee(exchangeName string, epoch int) {
	/*
		creates directory committee if not yet created otherwise informs all the directory members
	*/
	if len(e.curDirectory) < c {

		e.isDirectory = true

		data := map[string]interface{}{"Identity": e.Identity}
		msg := map[string]interface{}{"data": data, "type": "directoryMember", "epoch": epoch}

		BroadcastToNetwork(exchangeName, msg)
		// change the state as it is the directory member
		e.state = ElasticoStates["RunAsDirectory"]
	} else {

		e.SendToDirectory(epoch)
		if e.state != ElasticoStates["Receiving Committee Members"] {

			e.state = ElasticoStates["Formed Committee"]
		}
	}
}

func (e *Elastico) verifyPoW(identityobj IDENTITY) bool {
	/*
		verify the PoW of the node identityobj
	*/
	zeroString := ""
	for i := 0; i < D; i++ {
		zeroString += "0"
	}
	PoW := identityobj.PoW
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
	// 	digest = e.hexdigest(Ri)
	// 	if digest not in e.RcommitmentSet:
	// 		return false

	// reconstruct epoch randomness
	EpochRandomness := identityobj.EpochRandomness
	listOfRs := PoW.SetOfRs
	// listOfRs := PoW["SetOfRs"].([]interface{})
	setOfRs := make([]string, len(listOfRs))
	for i := range listOfRs {
		setOfRs[i] = listOfRs[i] //.(string)
	}

	if len(setOfRs) > 0 {
		xorVal := xorbinary(setOfRs)
		EpochRandomness = fmt.Sprintf("%0"+strconv.FormatInt(r, 10)+"b\n", xorVal)
	}

	// recompute PoW

	// public key
	rsaPublickey := identityobj.PK
	IP := identityobj.IP
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
	if strings.HasPrefix(hashVal, zeroString) && hashVal == hash {
		// Found a valid Pow, If this doesn't match with PoW["Hash"] then Doesnt verify!
		return true
	}
	logger.Error("POW not verified - prefix not found")
	return false

}

func (e *Elastico) runPBFT(epoch int) {
	/*
		Runs a Pbft instance for the intra-committee consensus
	*/
	if e.state == ElasticoStates["PBFT_NONE"] {
		if e.primary {
			prePrepareMsg := e.constructPrePrepare(epoch) //construct pre-prepare msg
			// multicasts the pre-prepare msg to replicas
			// ToDo: what if primary does not send the pre-prepare to one of the nodes
			e.sendPrePrepare(prePrepareMsg)

			// change the state of primary to pre-prepared
			e.state = ElasticoStates["PBFT_PRE_PREPARE_SENT"]
			// primary will log the pre-prepare msg for itself
			prePrepareMsgEncoded, _ := json.Marshal(prePrepareMsg["data"])

			var decodedMsg PrePrepareMsg
			err := json.Unmarshal(prePrepareMsgEncoded, &decodedMsg)
			connection.FailOnError(err, "error in unmarshal pre-prepare", true)
			e.logPrePrepareMsg(decodedMsg)

		} else {

			// for non-primary members
			if e.isPrePrepared() {
				e.state = ElasticoStates["PBFT_PRE_PREPARE"]
			}
		}

	} else if e.state == ElasticoStates["PBFT_PRE_PREPARE"] {

		if e.primary == false {

			// construct prepare msg
			// ToDo: verify whether the pre-prepare msg comes from various primaries or not
			preparemsgList := e.constructPrepare(epoch)
			e.sendPrepare(preparemsgList)
			e.state = ElasticoStates["PBFT_PREPARE_SENT"]
		}

	} else if e.state == ElasticoStates["PBFT_PREPARE_SENT"] || e.state == ElasticoStates["PBFT_PRE_PREPARE_SENT"] {
		// ToDo: if, primary has not changed its state to "PBFT_PREPARE_SENT"
		if e.isPrepared() {

			// logging.warning("prepared done by %s" , str(e.Port))
			e.state = ElasticoStates["PBFT_PREPARED"]
		}

	} else if e.state == ElasticoStates["PBFT_PREPARED"] {

		commitMsgList := e.constructCommit(epoch)
		e.sendCommit(commitMsgList)
		e.state = ElasticoStates["PBFT_COMMIT_SENT"]

	} else if e.state == ElasticoStates["PBFT_COMMIT_SENT"] {

		if e.isCommitted() {

			// logging.warning("committed done by %s" , str(e.Port))
			e.state = ElasticoStates["PBFT_COMMITTED"]
		}
	}

}

func (e *Elastico) isCommitted() bool {
	/*
		Check if the state is committed or not
	*/
	// collect committed data
	committedData := make(map[int]map[int][]Transaction)
	f := (c - 1) / 3
	// check for received request messages
	for socket := range e.prePrepareMsgLog {
		prePrepareMsg := e.prePrepareMsgLog[socket]
		// In current View Id
		if prePrepareMsg.PrePrepareData.ViewID == e.viewID {
			// request msg of pre-prepare request
			requestMsg := prePrepareMsg.Message
			// digest of the message
			digest := prePrepareMsg.PrePrepareData.Digest
			// get sequence number of this msg
			seqnum := prePrepareMsg.PrePrepareData.Seq

			if _, ok := e.preparedData[e.viewID]; ok == true {
				_, okk := e.preparedData[e.viewID][seqnum]
				if okk == true {
					if txnHexdigest(e.preparedData[e.viewID][seqnum]) == digest {
						// pre-prepared matched and prepared is also true, check for commits
						if _, okkk := e.commitMsgLog[e.viewID]; okkk == true {

							if _, okkkk := e.commitMsgLog[e.viewID][seqnum]; okkkk == true {

								count := 0
								for replicaID := range e.commitMsgLog[e.viewID][seqnum] {

									for _, msg := range e.commitMsgLog[e.viewID][seqnum][replicaID] {

										if msg.Digest == digest {

											count++
											break
										}
									}
								}
								// ToDo: condition check
								if count >= 2*f+1 {

									if _, presentView := committedData[e.viewID]; presentView == false {

										committedData[e.viewID] = make(map[int][]Transaction)
									}
									if _, presentSeq := committedData[e.viewID][seqnum]; presentSeq == false {

										committedData[e.viewID][seqnum] = make([]Transaction, 0)
									}

									for _, txn := range requestMsg {

										committedData[e.viewID][seqnum] = append(committedData[e.viewID][seqnum], txn)
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
					logger.Error("seqnum not found in isCommitted")
				}

			} else {

				logger.Error("view not found in isCommitted")
			}
		} else {

			logger.Error("wrong view in is committed")
		}
	}

	if len(committedData) > 0 {

		e.committedData = committedData
		logger.Info("committed check done by port - ", e.Port)
		return true
	}
	return false
}

func (e *Elastico) logPrePrepareMsg(msg PrePrepareMsg) {
	/*
		log the pre-prepare msg
	*/
	identityobj := msg.Identity
	IP := identityobj.IP
	Port := identityobj.Port
	// create a socket
	socket := IP + ":" + Port
	e.prePrepareMsgLog[socket] = msg
	logger.Info("logging the pre-prepare--", e.prePrepareMsgLog)
}

func (e *Elastico) logFinalPrePrepareMsg(msg PrePrepareMsg) {
	/*
		log the pre-prepare msg
	*/
	identityobj := msg.Identity
	IP := identityobj.IP
	Port := identityobj.Port
	// create a socket
	socket := IP + ":" + Port
	e.FinalPrePrepareMsgLog[socket] = msg

}

func (e *Elastico) isPrepared() bool {
	/*
		Check if the state is prepared or not
	*/
	// collect prepared data
	preparedData := make(map[int]map[int][]Transaction)
	f := (c - 1) / 3
	// check for received request messages
	for socket := range e.prePrepareMsgLog {

		// In current View Id
		socketMap := e.prePrepareMsgLog[socket]
		prePrepareData := socketMap.PrePrepareData
		if prePrepareData.ViewID == e.viewID {

			// request msg of pre-prepare request
			requestMsg := socketMap.Message
			// digest of the message
			digest := prePrepareData.Digest
			// get sequence number of this msg
			seqnum := prePrepareData.Seq
			// find Prepare msgs for this view and sequence number
			_, ok := e.prepareMsgLog[e.viewID]

			if ok == true {
				prepareMsgLogViewID := e.prepareMsgLog[e.viewID]
				_, okk := prepareMsgLogViewID[seqnum]
				if okk == true {
					// need to find matching prepare msgs from different replicas atleast c/2 + 1
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

						if _, ok := preparedData[e.viewID]; ok == false {

							preparedData[e.viewID] = make(map[int][]Transaction)
						}
						preparedViewID := preparedData[e.viewID]
						if _, ok := preparedViewID[seqnum]; ok == false {

							preparedViewID[seqnum] = make([]Transaction, 0)
						}
						for _, txn := range requestMsg {

							preparedViewID[seqnum] = append(preparedViewID[seqnum], txn)
						}
						preparedData[e.viewID][seqnum] = preparedViewID[seqnum]
					}
				}
			}

		}
	}
	if len(preparedData) > 0 {
		e.preparedData = preparedData
		logger.Info("prepared check done")
		return true
	}

	return false
}

func (e *Elastico) runFinalPBFT(epoch int) {
	/*
		Run PBFT by final committee members
	*/
	if e.state == ElasticoStates["FinalPBFT_NONE"] {

		if e.primary {

			fmt.Println("port of final primary- ", e.Port)
			// construct pre-prepare msg
			finalPrePreparemsg := e.constructFinalPrePrepare(epoch)
			// multicasts the pre-prepare msg to replicas
			e.sendPrePrepare(finalPrePreparemsg)

			// change the state of primary to pre-prepared
			e.state = ElasticoStates["FinalPBFT_PRE_PREPARE_SENT"]
			// primary will log the pre-prepare msg for itself

			prePrepareMsgEncoded, _ := json.Marshal(finalPrePreparemsg["data"])

			var decodedMsg PrePrepareMsg
			err := json.Unmarshal(prePrepareMsgEncoded, &decodedMsg)
			connection.FailOnError(err, "error in unmarshal final pre-prepare", true)

			e.logFinalPrePrepareMsg(decodedMsg)

		} else {

			// for non-primary members
			if e.isFinalprePrepared() {
				e.state = ElasticoStates["FinalPBFT_PRE_PREPARE"]
			}
		}

	} else if e.state == ElasticoStates["FinalPBFT_PRE_PREPARE"] {

		if e.primary == false {

			// construct prepare msg
			FinalpreparemsgList := e.constructFinalPrepare(epoch)
			e.sendPrepare(FinalpreparemsgList)
			e.state = ElasticoStates["FinalPBFT_PREPARE_SENT"]
		}
	} else if e.state == ElasticoStates["FinalPBFT_PREPARE_SENT"] || e.state == ElasticoStates["FinalPBFT_PRE_PREPARE_SENT"] {

		// ToDo: primary has not changed its state to "FinalPBFT_PREPARE_SENT"
		if e.isFinalPrepared() {

			fmt.Println("final prepared done")
			e.state = ElasticoStates["FinalPBFT_PREPARED"]
		}
	} else if e.state == ElasticoStates["FinalPBFT_PREPARED"] {

		commitMsgList := e.constructFinalCommit(epoch)
		e.sendCommit(commitMsgList)
		e.state = ElasticoStates["FinalPBFT_COMMIT_SENT"]

	} else if e.state == ElasticoStates["FinalPBFT_COMMIT_SENT"] {

		if e.isFinalCommitted() {
			for viewID := range e.FinalcommittedData {

				for seqnum := range e.FinalcommittedData[viewID] {

					msgList := e.FinalcommittedData[viewID][seqnum]

					e.finalBlock.Txns = e.unionTxns(e.finalBlock.Txns, msgList)
				}
			}
			e.state = ElasticoStates["FinalPBFT_COMMITTED"]
		}
	}
}

func (e *Elastico) isFinalPrepared() bool {
	/*
		Check if the state is prepared or not
	*/
	//  collect prepared data
	preparedData := make(map[int]map[int][]Transaction)
	f := (c - 1) / 3
	//  check for received request messages
	for socket := range e.FinalPrePrepareMsgLog {

		//  In current View Id
		socketMap := e.FinalPrePrepareMsgLog[socket]
		prePrepareData := socketMap.PrePrepareData
		if prePrepareData.ViewID == e.viewID {

			//  request msg of pre-prepare request
			requestMsg := socketMap.Message

			//  digest of the message
			digest := prePrepareData.Digest
			//  get sequence number of this msg
			seqnum := prePrepareData.Seq
			//  find Prepare msgs for this view and sequence number
			if _, presentView := e.FinalPrepareMsgLog[e.viewID]; presentView == true {
				prepareMsgLogViewID := e.FinalPrepareMsgLog[e.viewID]
				if _, presentSeq := prepareMsgLogViewID[seqnum]; presentSeq == true {

					//  need to find matching prepare msgs from different replicas atleast c//2 + 1
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

						if _, ok := preparedData[e.viewID]; ok == false {

							preparedData[e.viewID] = make(map[int][]Transaction)
						} else {
							logger.Error("view not there in prepared data")
						}
						preparedViewID := preparedData[e.viewID]
						if _, ok := preparedViewID[seqnum]; ok == false {

							preparedViewID[seqnum] = make([]Transaction, 0)
						} else {
							logger.Error("seq not there in prepared data")
						}
						for _, txn := range requestMsg {

							preparedViewID[seqnum] = append(preparedViewID[seqnum], txn)
						}
						preparedData[e.viewID][seqnum] = preparedViewID[seqnum]
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
	if len(preparedData) > 0 {

		e.FinalpreparedData = preparedData
		return true
	}
	logger.Error("len of prepared data == 0")
	return false
}

func (e *Elastico) isFinalCommitted() bool {
	/*
		Check if the state is committed or not
	*/
	// collect committed data
	committedData := make(map[int]map[int][]Transaction)
	f := (c - 1) / 3
	// check for received request messages
	for socket := range e.FinalPrePrepareMsgLog {
		prePrepareMsg := e.FinalPrePrepareMsgLog[socket]
		// In current View Id
		if prePrepareMsg.PrePrepareData.ViewID == e.viewID {
			// request msg of pre-prepare request
			requestMsg := prePrepareMsg.Message
			// digest of the message
			digest := prePrepareMsg.PrePrepareData.Digest
			// get sequence number of this msg
			seqnum := prePrepareMsg.PrePrepareData.Seq
			if _, presentView := e.FinalpreparedData[e.viewID]; presentView == true {
				if _, presentSeq := e.FinalpreparedData[e.viewID][seqnum]; presentSeq == true {
					if txnHexdigest(e.FinalpreparedData[e.viewID][seqnum]) == digest {
						// pre-prepared matched and prepared is also true, check for commits
						if _, ok := e.FinalcommitMsgLog[e.viewID]; ok == true {
							if _, okk := e.FinalcommitMsgLog[e.viewID][seqnum]; okk == true {
								count := 0
								for replicaID := range e.FinalcommitMsgLog[e.viewID][seqnum] {
									for _, msg := range e.FinalcommitMsgLog[e.viewID][seqnum][replicaID] {

										if msg.Digest == digest {
											count++
											break
										}
									}
								}
								// ToDo: condition check
								if count >= 2*f+1 {
									if _, presentview := committedData[e.viewID]; presentview == false {

										committedData[e.viewID] = make(map[int][]Transaction)
									}
									if _, presentSeq := committedData[e.viewID][seqnum]; presentSeq == false {

										committedData[e.viewID][seqnum] = make([]Transaction, 0)
									}
									for _, txn := range requestMsg {

										committedData[e.viewID][seqnum] = append(committedData[e.viewID][seqnum], txn)
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

					logger.Error("seq not found in isCommitted")
				}
			} else {

				logger.Error("view not found in isCommitted")
			}
		} else {

			logger.Error("wrong view in is committed")
		}
	}
	if len(committedData) > 0 {

		e.FinalcommittedData = committedData
		return true
	}
	return false
}

func (e *Elastico) isPrePrepared() bool {
	/*
		if the node received the pre-prepare msg from the primary
	*/
	return len(e.prePrepareMsgLog) > 0
}

func (e *Elastico) isFinalprePrepared() bool {

	return len(e.FinalPrePrepareMsgLog) > 0
}

// txnHexdigest - Hex digest of txn List
func txnHexdigest(txnList []Transaction) string {
	/*
		return hexdigest for a list of transactions
	*/
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
	signed, err := rsa.SignPKCS1v15(rand.Reader, e.key, crypto.SHA256, digest) // sign the digest
	connection.FailOnError(err, "Error in Signing byte array", true)
	signature := base64.StdEncoding.EncodeToString(signed) // encode to base64
	return signature
}

func (e *Elastico) constructPrePrepare(epoch int) map[string]interface{} {
	/*
		construct pre-prepare msg , done by primary
	*/
	txnBlockList := e.txnBlock
	// ToDo: make prePrepareContents Ordered Dict for signatures purpose
	prePrepareContents := PrePrepareContents{Type: "pre-prepare", ViewID: e.viewID, Seq: 1, Digest: txnHexdigest(txnBlockList)}

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

func (e *Elastico) constructPrepare(epoch int) []map[string]interface{} {
	/*
		construct prepare msg in the prepare phase
	*/
	prepareMsgList := make([]map[string]interface{}, 0)
	//  loop over all pre-prepare msgs
	for socketID := range e.prePrepareMsgLog {

		msg := e.prePrepareMsgLog[socketID]
		prePreparedData := msg.PrePrepareData
		seqnum := prePreparedData.Seq
		digest := prePreparedData.Digest

		//  make prepare_contents Ordered Dict for signatures purpose
		prepareContents := PrepareContents{Type: "prepare", ViewID: e.viewID, Seq: seqnum, Digest: digest}
		PrepareContentsDigest := e.digestPrepareMsg(prepareContents)
		data := map[string]interface{}{"PrepareData": prepareContents, "Sign": e.Sign(PrepareContentsDigest), "Identity": e.Identity}
		preparemsg := map[string]interface{}{"data": data, "type": "prepare", "epoch": epoch}
		prepareMsgList = append(prepareMsgList, preparemsg)
	}
	return prepareMsgList

}

func (e *Elastico) constructFinalPrepare(epoch int) []map[string]interface{} {
	/*
		construct prepare msg in the prepare phase
	*/
	FinalprepareMsgList := make([]map[string]interface{}, 0)
	for socketID := range e.FinalPrePrepareMsgLog {

		msg := e.FinalPrePrepareMsgLog[socketID]
		prePreparedData := msg.PrePrepareData
		seqnum := prePreparedData.Seq
		digest := prePreparedData.Digest
		//  make prepare_contents Ordered Dict for signatures purpose

		prepareContents := PrepareContents{Type: "Finalprepare", ViewID: e.viewID, Seq: seqnum, Digest: digest}
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

func (e *Elastico) constructCommit(epoch int) []map[string]interface{} {
	/*
		Construct commit msgs
	*/
	commitMsges := make([]map[string]interface{}, 0)

	for viewID := range e.preparedData {

		for seqnum := range e.preparedData[viewID] {

			digest := txnHexdigest(e.preparedData[viewID][seqnum])
			// make commit_contents Ordered Dict for signatures purpose
			commitContents := CommitContents{Type: "commit", ViewID: viewID, Seq: seqnum, Digest: digest}
			commitContentsDigest := e.digestCommitMsg(commitContents)
			data := map[string]interface{}{"Sign": e.Sign(commitContentsDigest), "CommitData": commitContents, "Identity": e.Identity}
			commitMsg := map[string]interface{}{"data": data, "type": "commit", "epoch": epoch}
			commitMsges = append(commitMsges, commitMsg)

		}
	}

	return commitMsges
}

func (e *Elastico) constructFinalCommit(epoch int) []map[string]interface{} {
	/*
		Construct commit msgs
	*/
	commitMsges := make([]map[string]interface{}, 0)

	for viewID := range e.FinalpreparedData {

		for seqnum := range e.FinalpreparedData[viewID] {

			digest := txnHexdigest(e.FinalpreparedData[viewID][seqnum])
			//  make commit_contents Ordered Dict for signatures purpose
			commitContents := CommitContents{Type: "Finalcommit", ViewID: viewID, Seq: seqnum, Digest: digest}
			commitContentsDigest := e.digestCommitMsg(commitContents)

			data := map[string]interface{}{"Sign": e.Sign(commitContentsDigest), "CommitData": commitContents, "Identity": e.Identity}
			commitMsg := map[string]interface{}{"data": data, "type": "Finalcommit", "epoch": epoch}
			commitMsges = append(commitMsges, commitMsg)

		}
	}

	return commitMsges
}

func (e *Elastico) digestPrePrepareMsg(msg PrePrepareContents) []byte {
	digest := sha256.New()
	digest.Write([]byte(msg.Type))
	digest.Write([]byte(strconv.Itoa(msg.ViewID)))
	digest.Write([]byte(strconv.Itoa(msg.Seq)))
	digest.Write([]byte(msg.Digest))
	return digest.Sum(nil)
}

func (e *Elastico) digestPrepareMsg(msg PrepareContents) []byte {
	digest := sha256.New()
	digest.Write([]byte(msg.Type))
	digest.Write([]byte(strconv.Itoa(msg.ViewID)))
	digest.Write([]byte(strconv.Itoa(msg.Seq)))
	digest.Write([]byte(msg.Digest))
	return digest.Sum(nil)
}

func (e *Elastico) digestCommitMsg(msg CommitContents) []byte {
	digest := sha256.New()
	digest.Write([]byte(msg.Type))
	digest.Write([]byte(strconv.Itoa(msg.ViewID)))
	digest.Write([]byte(strconv.Itoa(msg.Seq)))
	digest.Write([]byte(msg.Digest))
	return digest.Sum(nil)
}

func (e *Elastico) constructFinalPrePrepare(epoch int) map[string]interface{} {
	/*
		construct pre-prepare msg , done by primary final
	*/
	txnBlockList := e.mergedBlock
	// ToDo :- make pre_prepare_contents Ordered Dict for signatures purpose
	prePrepareContents := PrePrepareContents{Type: "Finalpre-prepare", ViewID: e.viewID, Seq: 1, Digest: txnHexdigest(txnBlockList)}

	prePrepareContentsDigest := e.digestPrePrepareMsg(prePrepareContents)

	data := map[string]interface{}{"Message": txnBlockList, "PrePrepareData": prePrepareContents, "Sign": e.Sign(prePrepareContentsDigest), "Identity": e.Identity}
	prePrepareMsg := map[string]interface{}{"data": data, "type": "Finalpre-prepare", "epoch": epoch}
	return prePrepareMsg

}

func (e *Elastico) sendPrePrepare(prePrepareMsg map[string]interface{}) {
	/*
		Send pre-prepare msgs to all committee members
	*/
	for _, nodeID := range e.committeeMembers {

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
	for _, commitMsg := range commitMsgList {

		for _, nodeID := range e.committeeMembers {

			nodeID.send(commitMsg)
		}
	}
}

func (e *Elastico) sendPrepare(prepareMsgList []map[string]interface{}) {
	/*
		send the prepare msgs to the committee members
	*/
	// send prepare msg list to committee members
	for _, preparemsg := range prepareMsgList {

		for _, nodeID := range e.committeeMembers {

			nodeID.send(preparemsg)
		}
	}
}

//Execute :-
func (e *Elastico) Execute(exchangeName string, epoch int) string {
	/*
		executing the functions based on the running state
	*/
	// initial state of elastico node
	if e.state == ElasticoStates["NONE"] {
		e.executePoW()
	} else if e.state == ElasticoStates["PoW Computed"] {

		// form Identity, when PoW computed
		e.formIdentity()
	} else if e.state == ElasticoStates["Formed Identity"] {

		// form committee, when formed Identity
		e.formCommittee(exchangeName, epoch)
	} else if e.isDirectory && e.state == ElasticoStates["RunAsDirectory"] {

		logger.Infof("The directory member :- %s ", e.Port)
		e.receiveTxns(epochTxn)
		// directory member has received the txns for all committees
		e.state = ElasticoStates["RunAsDirectory after-TxnReceived"]
	} else if e.state == ElasticoStates["Receiving Committee Members"] {

		// when a node is part of some committee
		if e.flag == false {

			// logging the bad nodes
			logger.Error("member with invalid POW %s with commMembers : %s", e.Identity, e.committeeMembers)
		}
		// Now The node should go for Intra committee consensus
		// initial state for the PBFT
		e.state = ElasticoStates["PBFT_NONE"]
		// run PBFT for intra-committee consensus
		e.runPBFT(epoch)

	} else if e.state == ElasticoStates["PBFT_NONE"] || e.state == ElasticoStates["PBFT_PRE_PREPARE"] || e.state == ElasticoStates["PBFT_PREPARE_SENT"] || e.state == ElasticoStates["PBFT_PREPARED"] || e.state == ElasticoStates["PBFT_COMMIT_SENT"] || e.state == ElasticoStates["PBFT_PRE_PREPARE_SENT"] {

		// run pbft for intra consensus
		e.runPBFT(epoch)
	} else if e.state == ElasticoStates["PBFT_COMMITTED"] {

		// send pbft consensus blocks to final committee members
		logger.Info("pbft finished by members", e.Port)
		e.SendtoFinal(epoch)

	} else if e.isFinalMember() && e.state == ElasticoStates["Intra Consensus Result Sent to Final"] {

		// final committee node will collect blocks and merge them
		e.checkCountForConsensusData()

	} else if e.isFinalMember() && e.state == ElasticoStates["Merged Consensus Data"] {

		// final committee member runs final pbft
		e.state = ElasticoStates["FinalPBFT_NONE"]
		fmt.Println("start pbft by final member with port--", e.Port)
		e.runFinalPBFT(epoch)

	} else if e.state == ElasticoStates["FinalPBFT_NONE"] || e.state == ElasticoStates["FinalPBFT_PRE_PREPARE"] || e.state == ElasticoStates["FinalPBFT_PREPARE_SENT"] || e.state == ElasticoStates["FinalPBFT_PREPARED"] || e.state == ElasticoStates["FinalPBFT_COMMIT_SENT"] || e.state == ElasticoStates["FinalPBFT_PRE_PREPARE_SENT"] {

		e.runFinalPBFT(epoch)

	} else if e.isFinalMember() && e.state == ElasticoStates["FinalPBFT_COMMITTED"] {

		// send the commitment to other final committee members
		e.sendCommitment(epoch)
		logger.Warn("pbft finished by final committee", e.Port)

	} else if e.isFinalMember() && e.state == ElasticoStates["CommitmentSentToFinal"] {

		// broadcast final txn block to ntw
		if len(e.commitments) >= c/2+1 {
			logger.Info("commitments received sucess")
			e.RunInteractiveConsistency(epoch)
		}
	} else if e.isFinalMember() && e.state == ElasticoStates["InteractiveConsistencyStarted"] {
		if len(e.EpochcommitmentSet) >= c/2+1 {
			e.state = ElasticoStates["InteractiveConsistencyAchieved"]
		} else {
			logger.Warn("Int. Consistency short : ", len(e.EpochcommitmentSet), " port :", e.Port)
		}
	} else if e.isFinalMember() && e.state == ElasticoStates["InteractiveConsistencyAchieved"] {

		// broadcast final txn block to ntw
		logger.Info("Consistency received sucess")
		e.BroadcastFinalTxn(epoch)
	} else if e.state == ElasticoStates["FinalBlockReceived"] {

		e.checkCountForFinalData()

	} else if e.isFinalMember() && e.state == ElasticoStates["FinalBlockSentToClient"] {

		// broadcast Ri is done when received commitment has atleast c/2  + 1 signatures
		if len(e.newRcommitmentSet) >= c/2+1 {
			logger.Info("broadacst R by port--", e.Port)
			e.BroadcastR(epoch)
		} else {
			logger.Info("insufficient Rs")
		}
	} else if e.state == ElasticoStates["BroadcastedR"] {
		if len(e.newsetOfRs) >= c/2+1 {
			logger.Info("received the set of Rs")
			e.state = ElasticoStates["ReceivedR"]
		}
		//  else {
		// 	logger.Info("Insuffice Set of Rs")
		// }
	} else if e.state == ElasticoStates["ReceivedR"] {

		e.appendToLedger()
		e.state = ElasticoStates["LedgerUpdated"]

	} else if e.state == ElasticoStates["LedgerUpdated"] {

		// Now, the node can be reset
		return "reset"
	}
	return ""
}
