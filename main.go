package main

import (
	"flag"
	"fmt"
	"log"
	"github.com/streadway/amqp"
)

func main() {
	var rabbitUrl string
	flag.StringVar(&rabbitUrl, "h", "amqp://localhost", "The URL of the AMQP server.")
	flag.Parse()

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
	err = channel.ExchangeDeclare("squall.workers", "direct", true, false, false, false, nil)

	if err != nil {
		panic(err)
	}

	// The exchange we're going to publish to...
	err = channel.ExchangeDeclare("squall.aggregators", "direct", true, false, false, false, nil)

	if err != nil {
		panic(err)
	}

	// Just ignore this crap.
	_, err = channel.QueueDeclare("scrape_requests", true, true, false, true, nil)

	if err != nil {
		panic(err)
	}

	// Start up dat consumer...!
	listener, err := channel.Consume("scrape_requests", "squall.workers", true, true, true, false, nil)

	if err != nil {
		panic(err)
	}

	for {
		select {
		case obj, ok := <-listener: {
			if ok {
				request := NewScrapeRequestFromJson(obj.Body)
				log.Printf("Request %d: Starting scrape of %s\n", request.RequestID, request.Url)
				request.PerformAsync(1000)
			}
		}
		case resp := <-ResponseQueue: {
			Debugln("Read response, sending along.")

			// Do something with this response. Shove it down another pipe?
			message := amqp.Publishing{}
			message.Body = resp.ToJSON()
			message.ContentType = "application/json"

			channel.Publish("squall.aggregators", "scrape_responses", false, false, message)
		}
		}
	}
}
