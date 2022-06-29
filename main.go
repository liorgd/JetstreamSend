package main

import (
	"encoding/json"
	"github.com/nats-io/nats.go"
	"log"
	"strconv"
)

const (
	streamName     = "ORDERS"
	streamSubjects = "ORDERS.*"
	subjectName    = "ORDERS.created"
)

func main() {
	log.Println("Connect to NATS")
	nc, _ := nats.Connect("demo.nats.io")
	log.Println("Creates JetStreamContext")
	js, err := nc.JetStream()
	checkErr(err)

	createStream(js)
	publishOrders(js)

}

type Order struct {
	OrderID    int
	CustomerID string
	Status     string
}

func createStream(js nats.JetStreamContext) {
	log.Printf("DeleteStream old stream: %q", streamName)
	js.DeleteStream(streamName)

	stream, _ := js.StreamInfo(streamName)
	if stream == nil {
		log.Printf("creating new stream %q and subjects %q", streamName, streamSubjects)
		_, err := js.AddStream(&nats.StreamConfig{
			Name:     streamName,
			Subjects: []string{streamSubjects},
		})
		checkErr(err)
	}
}

func publishOrders(js nats.JetStreamContext) {
	var order Order
	for i := 1; i <= 10; i++ {
		order = Order{
			OrderID:    i,
			CustomerID: "Cust-" + strconv.Itoa(i),
			Status:     "created",
		}
		orderJSON, _ := json.Marshal(order)
		_, err := js.Publish(subjectName, orderJSON)
		checkErr(err)
		log.Printf("Published orderJSON:%s to stream:%q subjectName:%q", string(orderJSON), streamName, subjectName)

	}
}

func checkErr(err error) {
	if err != nil {
		log.Panic(err)
	}
}
