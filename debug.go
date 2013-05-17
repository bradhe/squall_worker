package main

import (
	"strings"
	"os"
	"log"
)

func IsDebug() bool {
	return !strings.EqualFold(os.Getenv("SQUALL_WORKER_DEBUG"), "")
}

func Debugf(format string, args ...interface{}) {
	if IsDebug() {
		log.Printf(format, args)
	}
}

func Debugln(format string) {
	if IsDebug() {
		log.Println(format)
	}
}
