package main

import (
	"os"
	"fmt"
	"log"
	"github.com/streadway/amqp"
)

func main() {
	rabbitUrl := os.Getenv("AMQP_URL")

	if rabbitUrl == "" {
		rabbitUrl = "amqp://localhost"
	}

	log.Println(fmt.Sprintf("Connecting to RabbitMQ (%s)", rabbitUrl))

	conn, err := amqp.Dial(rabbitUrl)

	if err != nil {
		panic(err)
	}

	defer conn.Close()

	channel, err := conn.Channel()

	if err != nil {
		panic(err)
	}

	defer channel.Close()

	// The exchange we're going to pull stuff from...
	err = channel.ExchangeDeclare("squall.workers", "fanout", true, false, false, false, nil)

	if err != nil {
		panic(err)
	}

	// The exchange we're going to publish to...
	err = channel.ExchangeDeclare("squall.aggregators", "direct", true, false, false, false, nil)

	if err != nil {
		panic(err)
	}

	// Just ignore this crap.
	_, err = channel.QueueDeclare("", true, false, false, true, nil)

	if err != nil {
		panic(err)
	}

	err = channel.QueueBind("", "#", "squall.workers", true, nil)

	if err != nil {
		panic(err)
	}

	// Start up dat consumer...!
	listener, err := channel.Consume("", "squall.workers", true, false, true, false, nil)

	if err != nil {
		panic(err)
	}

	for {
		select {
		case obj, ok := <-listener: {
			Debugln("Found request to execute.")

			if ok {
				request := NewScrapeRequestFromJson(obj.Body)
				log.Printf("Request %d: Starting scrape of %s\n", request.RequestID, request.Url)
				request.PerformAsync(100)
			}
		}
		case resp := <-ResponseQueue: {
			Debugln("Read response, sending along.")

			// Do something with this response. Shove it down another pipe?
			message := amqp.Publishing{}
			message.Body = resp.ToJSON()
			message.ContentType = "application/json"

			channel.Publish("squall.aggregators", "", false, false, message)
		}
		}
	}
}
