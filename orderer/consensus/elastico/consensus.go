/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package elastico

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"time"

	"github.com/hyperledger/fabric/orderer/consensus"
	"github.com/hyperledger/fabric/orderer/consensus/migration"
	cb "github.com/hyperledger/fabric/protos/common"
	"github.com/streadway/amqp"
)

// Transaction :-
type Transaction struct {
	ConfigSeq uint64
	Txn       cb.Envelope
}

// DummyMessage :-
type DummyMessage struct {
	ConfigSeq uint64
	Txn       *cb.Envelope
}

type consenter struct{}

type chain struct {
	support         consensus.ConsenterSupport
	sendChan        chan *Message
	exitChan        chan struct{}
	migrationStatus migration.Status
}

// Message :-
type Message struct {
	ConfigSeq uint64
	NormalMsg *cb.Envelope
	ConfigMsg *cb.Envelope
}

// New creates a new consenter for the solo consensus scheme.
// The solo consensus scheme is very simple, and allows only one consenter for a given chain (this process).
// It accepts messages being delivered via Order/Configure, orders them, and then uses the blockcutter to form the messages
// into blocks before writing to the given ledger
func New() consensus.Consenter {
	logger.Info("file:- consensus.go, func:- New()")
	return &consenter{}
}

func (solo *consenter) HandleChain(support consensus.ConsenterSupport, metadata *cb.Metadata) (consensus.Chain, error) {
	logger.Info("file:- consensus.go, func:- HandleChain()")
	return newChain(support), nil
}

func newChain(support consensus.ConsenterSupport) *chain {
	logger.Info("file:- consensus.go, func:- newChain()")
	return &chain{
		support:         support,
		sendChan:        make(chan *Message),
		exitChan:        make(chan struct{}),
		migrationStatus: migration.NewStatusStepper(support.IsSystemChannel(), support.ChainID()),
	}
}

func (ch *chain) Start() {
	logger.Info("file:- consensus.go, func:- Start()")
	go ch.main()
}

func (ch *chain) Halt() {
	logger.Info("file:- consensus.go, func:- Halt()")
	select {
	case <-ch.exitChan:
		// Allow multiple halts without panic
	default:
		close(ch.exitChan)
	}
}

func (ch *chain) WaitReady() error {
	logger.Info("file:- consensus.go, func:- WaitReady()")
	return nil
}

// Order accepts normal messages for ordering
func (ch *chain) Order(env *cb.Envelope, ConfigSeq uint64) error {
	logger.Info("file:- consensus.go, func:- Order()")
	select {
	case ch.sendChan <- &Message{
		ConfigSeq: ConfigSeq,
		NormalMsg: env,
	}:
		return nil
	case <-ch.exitChan:
		return fmt.Errorf("Exiting")
	}
}

// Configure accepts configuration update messages for ordering
func (ch *chain) Configure(config *cb.Envelope, ConfigSeq uint64) error {
	logger.Info("file:- consensus.go, func:- Configure()")
	select {
	case ch.sendChan <- &Message{
		ConfigSeq: ConfigSeq,
		ConfigMsg: config,
	}:
		return nil
	case <-ch.exitChan:
		return fmt.Errorf("Exiting")
	}
}

// Errored only closes on exit
func (ch *chain) Errored() <-chan struct{} {
	logger.Info("file:- consensus.go, func:- Errored()")
	return ch.exitChan
}

func (ch *chain) MigrationStatus() migration.Status {
	logger.Info("file:- consensus.go, func:- MigrationStatus()")
	return ch.migrationStatus
}

func marshalData(msg map[string]interface{}) []byte {
	body, err := json.Marshal(msg)
	FailOnError(err, "error in marshal", true)
	return body
}

// PublishMsg :- publish the message in the queue
func PublishMsg(channel *amqp.Channel, queueName string, msg map[string]interface{}) {

	body := marshalData(msg)

	err := channel.Publish(
		"",        // exchange
		queueName, // routing key
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        body,
		})

	FailOnError(err, "Failed to publish a Message", true)
}

type msgType struct {
	Data map[string]interface{}
	Type string
}

// EState :
type EState struct {
	State string
}

// GetState :- get the state of Elastico
func GetState(path string) string {

	if _, err := os.Stat(path); err == nil {
		file, errOpen := os.Open(path)
		FailOnError(errOpen, "error in opening the file", true)
		defer file.Close()
		decoder := json.NewDecoder(file)
		config := EState{}
		err := decoder.Decode(&config)
		if err != nil {
			return "" // file empty
		}
		return config.State
	}
	return ""
}

// SetState :-
func SetState(config EState, path string) {

	if _, err := os.Stat(path); os.IsNotExist(err) {
		file, err1 := os.Create(path)
		FailOnError(err1, "fail to create", true)
		defer file.Close()
	}
	data, errMarshal := json.Marshal(config)
	FailOnError(errMarshal, "error in marshalling the data", true)
	err2 := ioutil.WriteFile(path, data, 0644)
	FailOnError(err2, "fail to write in file", true)
}

// DeclareQueue :-
func DeclareQueue(channel *amqp.Channel, queueName string) {

	_, err := channel.QueueDeclare(
		queueName, //name of the queue
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)

	FailOnError(err, "Failed to declare a delivery queue", true)
}

func (ch *chain) runElastico(msg Transaction) []DummyMessage {

	conn := GetConnection()     // get the connection from rabbitmq
	channel := GetChannel(conn) // get the channel
	defer conn.Close()
	defer channel.Close()

	deliveryqueueName := "deliveryQueue"
	DeclareQueue(channel, deliveryqueueName)
	// path of the file of the elastico state
	path := "/conf.json"
	// elastico state
	// config := EState{}

	//construct the new epoch msg
	newEpochMsg := make(map[string]interface{})
	newEpochMsg["Type"] = "StartNewEpoch"
	newEpochMsg["Epoch"] = RandomGen(64).String()
	newEpochMsg["Data"] = msg
	newEpochMsg["Orderer"] = os.Getenv("ORDERER_HOST")
	// if elastico is running for previous epoch then wait for it to reset and get finished

	if stateEnv := GetState(path); stateEnv != "" && stateEnv != strconv.Itoa(ElasticoStates["Reset"]) {
		PublishMsg(channel, os.Getenv("ORDERER_HOST"), newEpochMsg)

	} else {

		// set the elastico state to NONE for next epoch
		// config.State = strconv.Itoa(ElasticoStates["NONE"])
		// SetState(config, path)

		// get all queues of rabbit mq
		allqueues := getallQueues()

		size := strconv.Itoa(len(msg.Txn.Payload))
		logger.Infof("Size of gng Payload %s", size)
		// inform other orderers to start the epoch
		for _, queueName := range allqueues {
			if queueName.Name != deliveryqueueName {
				PublishMsg(channel, queueName.Name, newEpochMsg)
			}
		}
	}
	// Block will not go to BlockCutter till state is reset for the orderer
	for StateEnv := GetState(path); StateEnv != strconv.Itoa(ElasticoStates["Reset"]); {
		StateEnv = GetState(path)
	}
	// get the messages from the delivery queue
	ListOfTxns := GetDeliveryMsg(channel, deliveryqueueName)
	return ListOfTxns
}

// DeliveryMsg :-
type DeliveryMsg struct {
	Type    string
	Epoch   string
	Data    []Transaction
	Orderer string
}

// GetDeliveryMsg :- get the messages out of the delivery queue
func GetDeliveryMsg(channel *amqp.Channel, queueName string) []DummyMessage {

	queue, err := channel.QueueInspect(queueName)
	FailOnError(err, "error in elivery queue inspect", true)
	var ListOfTxns []DummyMessage
	for ; queue.Messages > 0; queue.Messages-- {
		// get the message from the queue
		msg, ok, err := channel.Get(queue.Name, true)
		FailOnError(err, "error in msg get of delivery queue", true)
		if ok {
			var decodemsg DeliveryMsg
			err = json.Unmarshal(msg.Body, &decodemsg)
			FailOnError(err, "error in unmarshall of the delivery msg", true)
			// pick the messages for the designated orderer
			if decodemsg.Orderer == os.Getenv("ORDERER_HOST") {
				for _, txndelivery := range decodemsg.Data {
					logger.Infof("received delivery msg size - %s", strconv.Itoa(len(txndelivery.Txn.Payload)))
					DummyTxnMessage := DummyMessage{txndelivery.ConfigSeq, &txndelivery.Txn}
					ListOfTxns = append(ListOfTxns, DummyTxnMessage)
				}
			} else {
				// Requeue the messages that are for other orderer
				errRequeue := channel.Publish(
					"",        // exchange
					queueName, // routing key
					false,     // mandatory
					false,     // immediate
					amqp.Publishing{
						ContentType: "text/plain",
						Body:        msg.Body,
					})
				FailOnError(errRequeue, "fail to requeue", true)
			}
		}
	}

	return ListOfTxns
}

func (ch *chain) main() {
	var timer <-chan time.Time
	var err error

	for {
		seq := ch.support.Sequence()
		err = nil
		select {
		case msg := <-ch.sendChan:
			if msg.ConfigMsg == nil {
				// NormalMsg
				if msg.ConfigSeq < seq {
					_, err = ch.support.ProcessNormalMsg(msg.NormalMsg)
					if err != nil {
						logger.Warningf("Discarding bad normal Message: %s", err)
						continue
					}
				}
				// run the elastico
				var newTxn Transaction
				newTxn.ConfigSeq = msg.ConfigSeq
				newTxn.Txn = *msg.NormalMsg
				receivedMsgs := ch.runElastico(newTxn)
				for _, txnMsg := range receivedMsgs {
					logger.Info("Elastico completed")

					batches, pending := ch.support.BlockCutter().Ordered(txnMsg.Txn)

					for _, batch := range batches {
						block := ch.support.CreateNextBlock(batch)
						ch.support.WriteBlock(block, nil)
					}

					switch {
					case timer != nil && !pending:
						// Timer is already running but there are no messages pending, stop the timer
						timer = nil
					case timer == nil && pending:
						// Timer is not already running and there are messages pending, so start it
						timer = time.After(ch.support.SharedConfig().BatchTimeout())
						logger.Debugf("Just began %s batch timer", ch.support.SharedConfig().BatchTimeout().String())
					default:
						// Do nothing when:
						// 1. Timer is already running and there are messages pending
						// 2. Timer is not set and there are no messages pending
					}
				}

			} else {
				// ConfigMsg
				if msg.ConfigSeq < seq {
					msg.ConfigMsg, _, err = ch.support.ProcessConfigMsg(msg.ConfigMsg)
					if err != nil {
						logger.Warningf("Discarding bad config Message: %s", err)
						continue
					}
				}
				batch := ch.support.BlockCutter().Cut()
				if batch != nil {
					block := ch.support.CreateNextBlock(batch)
					ch.support.WriteBlock(block, nil)
				}

				block := ch.support.CreateNextBlock([]*cb.Envelope{msg.ConfigMsg})
				ch.support.WriteConfigBlock(block, nil)
				timer = nil
			}
		case <-timer:
			//clear the timer
			timer = nil

			batch := ch.support.BlockCutter().Cut()
			if len(batch) == 0 {
				logger.Warningf("Batch timer expired with no pending requests, this might indicate a bug")
				continue
			}
			logger.Debugf("Batch timer expired, creating block")
			block := ch.support.CreateNextBlock(batch)
			ch.support.WriteBlock(block, nil)
		case <-ch.exitChan:
			logger.Debugf("Exiting")
			return
		}
	}
}
