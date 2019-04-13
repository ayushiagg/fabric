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
	if err == nil {
		// consume all the messages one by one
		var requeueMsgs [][]byte
		for ; Queue.Messages > 0; Queue.Messages-- {

			// get the message from the queue
			msg, ok, err := ch.Get(Queue.Name, true)
			failOnError(err, "error in get of queue", true)
			if ok {
				err := json.Unmarshal(msg.Body, &decodedmsg)
				failOnError(err, "error in unmarshall", true)

				if decodedmsg.Epoch == epoch {
					// consume the msg by taking the action in receive
					e.receive(decodedmsg, epoch)
				} else if decodedmsg.Epoch > epoch {
					requeueMsgs = append(requeueMsgs, msg.Body)
					log.Warn("Need to requeue msgs! type - ", decodedmsg.Type, " epoch - ", decodedmsg.Epoch, " present epoch : ", epoch)
				} else {
					log.Warn("Discarding Msgs type - ", decodedmsg.Type, " epoch - ", decodedmsg.Epoch, " present epoch : ", epoch)
				}
			}
		}
		// requeue the messages of future epochs
		for _, msg := range requeueMsgs {
			err := ch.Publish(
				"",         // exchange
				Queue.Name, // routing key
				false,      // mandatory
				false,      // immediate
				amqp.Publishing{
					ContentType: "text/plain",
					Body:        msg,
				})
			failOnError(err, "fail to requeue", true)
		}
	}

}
