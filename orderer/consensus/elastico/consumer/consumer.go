package main

import (
	"encoding/json"
	"os"
	"strconv"

	"github.com/hyperledger/fabric/common/flogging"
	elastico "github.com/hyperledger/fabric/orderer/consensus/elastico"
	"github.com/streadway/amqp"
)

var logger = flogging.MustGetLogger("orderer.consensus.elastico.consumer")

// StateName :- returns the elastico state name corresponding to the elastico state number
func StateName(stateNum int) string {
	for k, v := range elastico.ElasticoStates {
		if v == stateNum {
			return k
		}
	}
	return "error-state"
}

//ExecuteConsume :-
func ExecuteConsume(ch *amqp.Channel, Queue string, decodeMsg elastico.DecodeMsgType, exchangeName string, newEpochMessage elastico.Transaction, elasticoObj *elastico.Elastico) {
	for {
		// get the statename corresponding to state number
		statename := StateName(elasticoObj.State)
		// logging the state name alongwith the orderer name
		logger.Infof("Orderer State - %s , %s ", os.Getenv("ORDERER_HOST"), statename)
		// Execute the steps of Elastico
		response := elasticoObj.Execute(exchangeName, decodeMsg.Epoch, newEpochMessage)

		if response == "Reset" {

			if os.Getenv("ORDERER_HOST") == decodeMsg.Orderer {

				var finalListMsg []elastico.Transaction
				logger.Infof("get response size : %s", strconv.Itoa(elasticoObj.GetResponseSize()))

				response := elasticoObj.GetResponse()
				for _, deliverBlock := range response {
					logger.Info("something in response")
					for _, txn := range deliverBlock.TxnList {
						logger.Infof("something in deliver block 's txnlist size - %s", strconv.Itoa(len(txn.Txn.Payload)))
						finalListMsg = append(finalListMsg, txn)
					}
				}
				data := make(map[string]interface{})
				data["Type"] = "deliveryMsg"
				data["Epoch"] = decodeMsg.Epoch
				data["Data"] = finalListMsg
				data["Orderer"] = decodeMsg.Orderer
				logger.Info("gng msg in delivery queue", data)
				elastico.PublishMsg(ch, "deliveryQueue", data)

			}
			// changing the state to reset in file after sending delivery msgs
			config := elastico.EState{}
			config.State = strconv.Itoa(elastico.ElasticoStates["Reset"])
			elastico.SetState(config, "/conf.json")
			logger.Infof("reset done by %s", os.Getenv("ORDERER_HOST"))
			elasticoObj.Reset()
			break
		}
		elasticoObj.Consume(ch, Queue, newEpochMessage, decodeMsg.Epoch)
	}
}

//Consume :-
func Consume(ch *amqp.Channel, queue amqp.Queue, elasticoObj *elastico.Elastico) {
	var decodemsg elastico.DecodeMsgType
	// consume all the messages one by one
	for ; queue.Messages > 0; queue.Messages-- {
		// get the message from the queue
		msg, ok, err := ch.Get(queue.Name, true)
		elastico.FailOnError(err, "error in getting msg from queue", true)
		if ok {
			err = json.Unmarshal(msg.Body, &decodemsg)
			elastico.FailOnError(err, "error in unmarshall the msg in get of the queue", true)
			if decodemsg.Type == "StartNewEpoch" {

				var newEpochMessage elastico.Transaction
				err := json.Unmarshal(decodemsg.Data, &newEpochMessage)
				elastico.FailOnError(err, "fail to decode new epoch msg", true)
				// set the exchange name as Epoch Num
				exchangeName := "epoch" + decodemsg.Epoch
				// declare the exchange
				errExchange := ch.ExchangeDeclare(exchangeName, "fanout", true, false, false, false, nil)
				elastico.FailOnError(errExchange, "Failed to declare a exchange", true)
				// bind the queue to the exchange
				errQueueBind := ch.QueueBind(queue.Name, "", exchangeName, false, nil)
				elastico.FailOnError(errQueueBind, "Failed to bind a queue to exchange", true)

				// testData(newEpochMessage)

				// consume the msg by taking the action in receive
				ExecuteConsume(ch, queue.Name, decodemsg, exchangeName, newEpochMessage, elasticoObj)
				break
			}
		}
	}
}

func testData(data elastico.Transaction) {
	logger.Infof("see the rcved configseq %s", strconv.FormatUint(data.ConfigSeq, 10))
	size := strconv.Itoa(len(data.Txn.Payload))
	logger.Infof("see the size of rcved Payload %s", size)
	if size != "0" {
		s := string(data.Txn.Payload)
		logger.Infof("see the sending array payloag %s", s)
	}
	os.Exit(1)
}

// Run :-
func Run(ch *amqp.Channel, queueName string, elasticoObj *elastico.Elastico) {
	defer ch.Close()
	for {
		// count the number of messages that are in the queue
		queue, err := ch.QueueInspect(queueName)
		elastico.FailOnError(err, "error in inspect", false)
		if err == nil {
			Consume(ch, queue, elasticoObj)
		}
	}
}

func main() {
	conn := elastico.GetConnection()
	ch := elastico.GetChannel(conn)
	defer conn.Close()

	// queue name is the container-id of the orderer
	queueName := os.Getenv("ORDERER_HOST")

	elastico.DeclareQueue(ch, queueName)

	elasticoObj := elastico.Elastico{} // create the Elastico Object
	elasticoObj.ElasticoInit()         // initialise the variables of Elastico
	logger.Info("elastico-consumer running start")
	Run(ch, queueName, &elasticoObj)
}
