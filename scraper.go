package main

import (
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

type runScrapeResponse struct {
	// The response we care about
	response	ScrapeResponse

	// Any relevant error
	err		error
}

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

	// A little logging to help.
	if resp.err != nil {
		log.Println(fmt.Sprintf("Request %d: Scrape failed.", self.RequestID))
	} else {
		log.Println(fmt.Sprintf("Request %d: Scrape complete in %s", self.RequestID, resp.response.ResponseTime))
	}

	return resp.response, resp.err
}

// Instantiate a new ScrapeRequest from a JSON byte string.
func NewScrapeRequestFromJson(str []byte) ScrapeRequest {
	var request ScrapeRequest
	json.Unmarshal(str, &request)
	return request
}
