package main

import (
	"encoding/json"
	"os"

	"github.com/hyperledger/fabric/common/flogging"
	elastico "github.com/hyperledger/fabric/orderer/consensus/elastico"
	"github.com/streadway/amqp"
)

var logger = flogging.MustGetLogger("orderer.consensus.elastico.consumer")

// StateName :-
func StateName(stateNum int) string {
	for k, v := range elastico.ElasticoStates {
		if v == stateNum {
			return k
		}
	}
	return "error-state"
}

//ExecuteConsume :-
func ExecuteConsume(ch *amqp.Channel, Queue string, decodeMsg elastico.DecodeMsgType, exchangeName string, newEpochMessage elastico.NewEpochMsg, elasticoObj *elastico.Elastico) {
	// logger.Info("file:- consumer.go, func:- ExecuteConsume()")
	for {
		statename := StateName(elasticoObj.State)
		logger.Infof("Orderer State - %s , %s ", os.Getenv("ORDERER_HOST"), statename)
		response := elasticoObj.Execute(exchangeName, decodeMsg.Epoch, newEpochMessage)
		if response == "reset" {
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
		elastico.FailOnError(err, "error in get of queue", true)
		if ok {
			err = json.Unmarshal(msg.Body, &decodemsg)
			elastico.FailOnError(err, "error in unmarshall", true)
			if decodemsg.Type == "StartNewEpoch" {

				var newEpochMessage elastico.NewEpochMsg
				err := json.Unmarshal(decodemsg.Data, &newEpochMessage)
				elastico.FailOnError(err, "fail to decode new epoch msg", true)
				// set he exchange name as Epoch Num
				exchangeName := decodemsg.Epoch
				// declare the exchange
				errExchange := ch.ExchangeDeclare(exchangeName, "fanout", true, false, false, false, nil)
				elastico.FailOnError(errExchange, "Failed to declare a exchange", true)
				// bind the queue to the exchange
				errQueueBind := ch.QueueBind(queue.Name, "", exchangeName, false, nil)
				elastico.FailOnError(errQueueBind, "Failed to bind a queue to exchange", true)

				// consume the msg by taking the action in receive
				ExecuteConsume(ch, queue.Name, decodemsg, exchangeName, newEpochMessage, elasticoObj)
				break
			}
		}
	}
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
			//ToDo:- Add reset for elastico node
		}
	}
}

func main() {
	conn := elastico.GetConnection()
	ch := elastico.GetChannel(conn)
	defer conn.Close()

	queueName := os.Getenv("ORDERER_HOST")

	queue, err := ch.QueueDeclare(
		queueName, //name of the queue
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)

	elastico.FailOnError(err, "Failed to declare a queue", true)
	elasticoObj := elastico.Elastico{}
	elasticoObj.ElasticoInit()
	logger.Info("elastico-consumer running start")
	Run(ch, queue.Name, &elasticoObj)
}
