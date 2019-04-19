package elastico

import (
	"encoding/json"
	"net/http"
	"strconv"
)

// Queue :-
type Queue struct {
	Name  string `json:"name"`
	VHost string `json:"vhost"`
}

func getallQueues() []Queue {
	logger.Info("file:- allqueues.go, func:- getallQueues()")
	manager := "rabbitmq0:15672/api/queues/"
	client := &http.Client{}
	req, _ := http.NewRequest("GET", manager, nil)
	req.SetBasicAuth("guest",
		"guest")
	resp, _ := client.Do(req)

	value := make([]Queue, 0)
	json.NewDecoder(resp.Body).Decode(&value)
	logger.Infof("value size %s", strconv.Itoa(len(value)))
	// fmt.Println(value)
	return value
}
