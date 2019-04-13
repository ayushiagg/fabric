package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/hyperledger/fabric/orderer/consensus/elastico/connection"
	"github.com/hyperledger/fabric/orderer/consensus/elastico/elasticoSteps"
	"github.com/streadway/amqp"
)

type msgType struct {
	Type string
	Data json.RawMessage
}

//ExecuteConsume :-
func ExecuteConsume(ch *amqp.Channel, Queue Queue, decodeMsg msgType, exchangeName string) {

	epoch := 0

	elasticoObj := elasticoSteps.Elastico{}
	elasticoObj.ElasticoInit()

	for {

		elasticoObj.Execute(exchangeName, epoch)
		elasticoObj.Consume(ch, Queue)
	}
}

//Consume :-
func Consume(ch *amqp.Channel, queue amqp.Queue, exchangeName string) {
	var decodedmsg msgType
	// consume all the messages one by one
	for ; queue.Messages > 0; queue.Messages-- {
		// get the message from the queue
		msg, ok, err := ch.Get(queue.Name, true)
		connection.FailOnError(err, "error in get of queue", true)
		if ok {
			err = json.Unmarshal(msg.Body, &decodedmsg)
			connection.FailOnError(err, "error in unmarshall", true)
			if decodedmsg.Type == "Start New Epoch" {
				// consume the msg by taking the action in receive
				ExecuteConsume(ch, queue, decodedmsg, exchangeName)
				break
			}
		}
	}
}

//Execute :-
func Execute(decodeMsg msgType) {
	/*
		executing the functions based on the running state
	*/

	el := elasticoSteps.Elastico{}

	// initial state of elastico node
	if e.state == ElasticoStates["NONE"] {
		e.executePoW()
	} else if e.state == ElasticoStates["PoW Computed"] {

		// form Identity, when PoW computed
		e.formIdentity()
	} else if e.state == ElasticoStates["Formed Identity"] {

		// form committee, when formed Identity
		e.formCommittee(epoch)
	} else if e.isDirectory && e.state == ElasticoStates["RunAsDirectory"] {

		log.Info("The directory member :- ", e.Port)
		e.receiveTxns(epochTxn)
		// directory member has received the txns for all committees
		e.state = ElasticoStates["RunAsDirectory after-TxnReceived"]
	} else if e.state == ElasticoStates["Receiving Committee Members"] {

		// when a node is part of some committee
		if e.flag == false {

			// logging the bad nodes
			log.Error("member with invalid POW %s with commMembers : %s", e.Identity, e.committeeMembers)
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
		log.Info("pbft finished by members", e.Port)
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
		log.Warn("pbft finished by final committee", e.Port)

	} else if e.isFinalMember() && e.state == ElasticoStates["CommitmentSentToFinal"] {

		// broadcast final txn block to ntw
		if len(e.commitments) >= c/2+1 {
			log.Info("commitments received sucess")
			e.RunInteractiveConsistency(epoch)
		}
	} else if e.isFinalMember() && e.state == ElasticoStates["InteractiveConsistencyStarted"] {
		if len(e.EpochcommitmentSet) >= c/2+1 {
			e.state = ElasticoStates["InteractiveConsistencyAchieved"]
		} else {
			log.Warn("Int. Consistency short : ", len(e.EpochcommitmentSet), " port :", e.Port)
		}
	} else if e.isFinalMember() && e.state == ElasticoStates["InteractiveConsistencyAchieved"] {

		// broadcast final txn block to ntw
		log.Info("Consistency received sucess")
		e.BroadcastFinalTxn(epoch)
	} else if e.state == ElasticoStates["FinalBlockReceived"] {

		e.checkCountForFinalData()

	} else if e.isFinalMember() && e.state == ElasticoStates["FinalBlockSentToClient"] {

		// broadcast Ri is done when received commitment has atleast c/2  + 1 signatures
		if len(e.newRcommitmentSet) >= c/2+1 {
			log.Info("broadacst R by port--", e.Port)
			e.BroadcastR(epoch)
		} else {
			log.Info("insufficient Rs")
		}
	} else if e.state == ElasticoStates["BroadcastedR"] {
		if len(e.newsetOfRs) >= c/2+1 {
			log.Info("received the set of Rs")
			e.state = ElasticoStates["ReceivedR"]
		}
		//  else {
		// 	log.Info("Insuffice Set of Rs")
		// }
	} else if e.state == ElasticoStates["ReceivedR"] {

		e.appendToLedger()
		e.state = ElasticoStates["LedgerUpdated"]

	} else if e.state == ElasticoStates["LedgerUpdated"] {

		// Now, the node can be reset
		return "reset"
	}

}
