package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
)

func main() {
	// url := "rabbitmq0"
	// url2 := "localhost"
	// conn, err := amqp.Dial("amqp://guest:guest@" + url + ":5672/")
	// fmt.Println(url2)
	// if err != nil {
	// 	fmt.Printf("error in connection %s", err)
	// }
	// ch, err2 := conn.Channel() // create a channel
	// if err2 != nil {
	// 	fmt.Printf("error in channel %s", err2)
	// }
	// queueName := os.Getenv("ORDERER_HOST")

	// queue, _ := ch.QueueDeclare(
	// 	queueName, //name of the queue
	// 	false,     // durable
	// 	false,     // delete when unused
	// 	false,     // exclusive
	// 	false,     // no-wait
	// 	nil,       // arguments
	// )
	// msg := "HELLO FROM RMQ!!!"
	// body, err := json.Marshal(msg)

	// err3 := ch.Publish(
	// 	"",         // exchange
	// 	queue.Name, // routing key
	// 	false,      // mandatory
	// 	false,      // immediate
	// 	amqp.Publishing{
	// 		ContentType: "text/plain",
	// 		Body:        body,
	// 	})
	// if err3 != nil {
	// 	fmt.Printf("error in publishing the msg %s", err3)
	// }
	// fmt.Println("")
	// setEnv("ELASTICO_STATE", "Envset")
	_ = getallQueues()

}

// EState :
type EState struct {
	State string
}

// Queue :-
type Queue struct {
	Name  string `json:"name"`
	VHost string `json:"vhost"`
}

func getallQueues() []Queue {
	manager := "http://rabbitmq0:15672/api/queues/"
	client := &http.Client{}
	req, _ := http.NewRequest("GET", manager, nil)
	req.SetBasicAuth("guest", "guest")
	resp, err2 := client.Do(req)
	value := make([]Queue, 0)
	if err2 != nil {
		fmt.Printf("error in client do %s", err2)
		return value
	}
	if resp != nil {
		json.NewDecoder(resp.Body).Decode(&value)
		fmt.Printf("value size %s\n", strconv.Itoa(len(value)))
	}
	return value
}

func setEnv(envName string, envVal string) {
	path := "/conf.json"
	var file *os.File
	config := EState{}
	if _, err := os.Stat(path); err == nil {
		// path/to/whatever exists
		file, _ = os.Open(path)
		decoder := json.NewDecoder(file)
		err := decoder.Decode(&config)
		if err != nil {
			fmt.Println("error:", err)
		}
		fmt.Println(config.State)
		config.State = "2"

	} else if os.IsNotExist(err) {
		// path/to/whatever does *not* exist
		file, _ = os.Create(path)
		config.State = "0"
	}

	defer file.Close()
	data, _ := json.Marshal(config)
	_ = ioutil.WriteFile(path, data, 0644)
}
