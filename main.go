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

	// Just ignore this crap.
	_, err = channel.QueueDeclare("squall.request", true, true, false, true, nil)

	if err != nil {
		panic(err)
	}

	// Start up dat consumer...!
	listener, err := channel.Consume("squall.request", "", true, true, true, false, nil)

	if err != nil {
		panic(err)
	}

	for {
		select {
		case obj, ok := <-listener: {
			if ok {
				request := NewScrapeRequestFromJson(obj.Body)

				log.Println(fmt.Sprintf("Request %d: Syncronously scraping %s", request.RequestID, request.Url))

				for i := 0; i < 50; i++ {
					request.Perform()
				}
			}
		}
		}
	}
}
