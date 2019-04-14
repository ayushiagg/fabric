package elastico

import (
	"encoding/json"
	"os"

	"github.com/streadway/amqp"
)

// DecodeMsgType :-
type DecodeMsgType struct {
	Type  string
	Data  json.RawMessage
	Epoch string
}

//ExecuteConsume :-
func ExecuteConsume(ch *amqp.Channel, Queue string, decodeMsg DecodeMsgType, exchangeName string, newEpochMessage NewEpochMsg, elasticoObj *Elastico) {

	for {

		response := elasticoObj.Execute(exchangeName, decodeMsg.Epoch, newEpochMessage)
		if response == "reset" {
			break
		}
		elasticoObj.Consume(ch, Queue, newEpochMessage, decodeMsg.Epoch)
	}
}

// NewEpochMsg   :-
type NewEpochMsg struct {
	Data *Message
}

//Consume :-
func Consume(ch *amqp.Channel, queue amqp.Queue, elasticoObj *Elastico) {
	var decodemsg DecodeMsgType
	// consume all the messages one by one
	for ; queue.Messages > 0; queue.Messages-- {
		// get the message from the queue
		msg, ok, err := ch.Get(queue.Name, true)
		FailOnError(err, "error in get of queue", true)

		if ok {
			err = json.Unmarshal(msg.Body, &decodemsg)
			FailOnError(err, "error in unmarshall", true)
			if decodemsg.Type == "Start New Epoch" {
				var newEpochMessage NewEpochMsg
				err := json.Unmarshal(decodemsg.Data, &newEpochMessage)
				FailOnError(err, "fail to decode new epoch msg", true)
				// set he exchange name as Epoch Num
				exchangeName := decodemsg.Epoch
				// declare the exchange
				errExchange := ch.ExchangeDeclare(exchangeName, "fanout", true, false, false, false, nil)
				FailOnError(errExchange, "Failed to declare a exchange", true)
				// bind the queue to the exchange
				errQueueBind := ch.QueueBind(queue.Name, "", exchangeName, false, nil)
				FailOnError(errQueueBind, "Failed to bind a queue to exchange", true)

				// consume the msg by taking the action in receive
				ExecuteConsume(ch, queue.Name, decodemsg, exchangeName, newEpochMessage, elasticoObj)
				break
			}
		}
	}
}

// Run :-
func Run(ch *amqp.Channel, queueName string, elasticoObj *Elastico) {
	defer ch.Close()
	for {
		// count the number of messages that are in the queue
		queue, err := ch.QueueInspect(queueName)
		FailOnError(err, "error in inspect", false)
		if err == nil {
			Consume(ch, queue, elasticoObj)
		}
	}
}

func main() {
	conn := GetConnection()
	ch := GetChannel(conn)
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

	FailOnError(err, "Failed to declare a queue", true)

	elasticoObj := Elastico{}
	elasticoObj.ElasticoInit()

	Run(ch, queue.Name, &elasticoObj)
}
