package main

import (
	"runtime"
	"log"
	"fmt"
	"strings"
	"encoding/json"
	"crypto/md5"
	"strconv"
	"time"
	"io/ioutil"
	"io"
	"net/http"
)

type ScrapeRequest struct {
	// The URL of the request
	Url		string

	// The ID of the request
	RequestID	int
}

type ScrapeResponse struct {
	// The original ID of the request.
	RequestID	int

	// How long the scrape took
	ResponseTime	time.Duration

	// When the scrape started.
	StartedAt	time.Time

	// The status code as a string that was returned.
	Status		string

	// The status code as an integer that was returned.
	StatusCode	int

	// The MD5 of the body returned by the server.
	MD5Sum		string
}

// Wrapper type for running scrapes asynchronously.
type runScrapeResponse struct {
	// The response we care about
	response	ScrapeResponse

	// Any relevant error
	err		error
}

var RequestQueue	chan(ScrapeRequest)
var ResponseQueue	chan(ScrapeResponse)


// Indicator as to whether or not the environment is fully configured. If true,
// assume that the procs are started for this. If false, assume that the
// Initialize() func needs to be called before scrape requests can be
// processed.
var initialized		bool

func (self ScrapeRequest) runScrape(ch chan runScrapeResponse) {
	response := ScrapeResponse{}
	response.RequestID = self.RequestID

	response.StartedAt = time.Now()
	resp, err := http.Get(self.Url)

	if err != nil {
		log.Println(fmt.Println("Request %d: Error during Get. %s", self.RequestID, err))
		ch <-runScrapeResponse{ScrapeResponse{}, err}

		// And get outta here.
		return
	}

	response.ResponseTime = time.Since(response.StartedAt);

	defer resp.Body.Close()

	response.Status = resp.Status

	fields := strings.Fields(response.Status)
	response.StatusCode, err = strconv.Atoi(fields[0])

	if err != nil {
		log.Println(fmt.Println("Request %d: Error parsing status code. %s", self.RequestID, err))
		ch <-runScrapeResponse{ScrapeResponse{}, err}

		// And get outta here.
		return
	}

	// Read in the body, we want to hash that.
	body, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		log.Println(fmt.Println("Request %d: Error reading body. %s", self.RequestID, err))
		ch <-runScrapeResponse{ScrapeResponse{}, err}

		// And get outta here.
		return
	}

	h := md5.New()
	io.WriteString(h, string(body))
	response.MD5Sum = fmt.Sprintf("%x", h.Sum(nil))

	ch <-runScrapeResponse{response, err}
}

// Synchronously perform a scrape for a ScrapeRequest.
func (self ScrapeRequest) Perform() (ScrapeResponse, error) {
	// We want to have space for only 1 element as we don't want to block
	// in the caller.
	ch := make(chan runScrapeResponse, 1)
	self.runScrape(ch)
	resp := <-ch

	return resp.response, resp.err
}

func (self ScrapeRequest) PerformAsync(n int) {
	if !initialized {
		Initialize()
	}

	for i := 0; i < n; i++ {
		RequestQueue <-self
	}
}

func (self ScrapeResponse) ToJSON() ([]byte) {
	bytes, err := json.Marshal(self)

	if err != nil {
		log.Println(fmt.Sprintf("Request %d: Marshalling to JSON failed. %s", self.RequestID, err))
	}

	return bytes
}

// Starts and runs a worker that pulls jobs off the RequestQueue queue.
func runQueueWorker() {
	for {
		select {
		case request := <-RequestQueue: {
			response, err := request.Perform()

			// A little logging to help.
			if err != nil {
				log.Println(fmt.Sprintf("Request %d: Scrape failed.", request.RequestID))
			} else {
				Debugf("Request %d: Back!", request.RequestID)
			}

			ResponseQueue <-response
		}
		}
	}
}

func Initialize() {
	n := (runtime.NumCPU() - 1)
	//n := 200

	// Make sure it's not TOO small, but want to leave 1 CPU for other app logic.
	if n < 1 {
		n = 1
	}

	RequestQueue	= make(chan(ScrapeRequest), n * 100)

	// Make the queue a bit bigger than request queue such that it can just
	// run away all on it's own.
	ResponseQueue	= make(chan(ScrapeResponse), n * 100)

	log.Println(fmt.Sprintf("Starting %d queue workers.", n))

	for i := 0; i < n; i++ {
		go runQueueWorker()
	}

	// Mark the whole thing as initialized!
	initialized = true
}

// Instantiate a new ScrapeRequest from a JSON byte string.
func NewScrapeRequestFromJson(str []byte) ScrapeRequest {
	var request ScrapeRequest
	json.Unmarshal(str, &request)

	return request
}
