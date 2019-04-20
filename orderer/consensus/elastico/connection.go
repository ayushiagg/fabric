package elastico

import (
	"os"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/streadway/amqp"
)

var logger = flogging.MustGetLogger("orderer.consensus.elastico")

// FailOnError :-
func FailOnError(err error, msg string, exit bool) {
	// logging the error
	if err != nil {
		logger.Infof("msg %s Error found! error:- %s", msg, err)
		if exit {
			os.Exit(1)
		}
	}
}

//GetChannel :-
func GetChannel(connection *amqp.Connection) *amqp.Channel {
	/*
		get channel
	*/
	channel, err := connection.Channel()               // create a channel
	FailOnError(err, "Failed to open a channel", true) // report the error
	return channel
}

//GetConnection :-
func GetConnection() *amqp.Connection {
	/*
		establish a connection with RabbitMQ server
	*/
	url := "rabbitmq0"
	connection, err := amqp.Dial("amqp://guest:guest@" + url + ":5672/")
	FailOnError(err, "Failed to connect to RabbitMQ", true) // report the error
	return connection
}
