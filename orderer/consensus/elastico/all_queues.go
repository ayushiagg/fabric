package elastico

import (
	"encoding/json"
	"net/http"
)

// Queue :-
type Queue struct {
	Name  string `json:"name"`
	VHost string `json:"vhost"`
}

func getallQueues() []Queue {
	manager := "http://rabbitmq0:15672/api/queues/" // rabbitmq API url
	client := &http.Client{}
	req, err1 := http.NewRequest("GET", manager, nil)
	FailOnError(err1, "error in NewRequest", true)

	req.SetBasicAuth("guest", "guest")
	resp, err2 := client.Do(req) // performs the HTTP request
	value := make([]Queue, 0)
	FailOnError(err2, "error in client do", true)
	if resp != nil {
		json.NewDecoder(resp.Body).Decode(&value)
		// logger.Infof("value size %s\n", strconv.Itoa(len(value)))
	}
	return value
}
