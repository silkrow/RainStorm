package main

import (
	"RainStormServer/failuredetector"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

func main() {
	// Signal handling for graceful shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)

	// Goroutine to handle shutdown signal
	go func() {
		<-c
		fmt.Println("\nReceived interrupt signal, shutting down.")
		os.Exit(0)
	}()

	if len(os.Args) < 2 {
		log.Fatal("Usage: go run . <vm_number>")
	}
	logFile, err := os.OpenFile("../machine.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open log file: %s", err)
	}
	defer logFile.Close()
	log.SetOutput(logFile)

	// Added for debugging (pprof)
	// Usage: go tool pprof http://localhost:6060/debug/pprof/profile

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	// Get the second argument which is the VM number
	vmNumber, err := strconv.Atoi(os.Args[1])
	if err != nil || vmNumber < 1 || vmNumber > 10 {
		log.Fatal("The VM number must be an integer between 1 and 10.")
	}

	//------------------------- Main Logic ------------------------//
	ml := failuredetector.NewMembershipList()
	// 1. Failure Detection Service
	go failuredetector.Failuredetect(ml, vmNumber)

	fs := FileServerInit(ml, vmNumber)
	// 2. Maintenance Daemon
	go Maintenance(fs)
	// 3. HTTP Request handling
	go HTTPServer(fs)

	st := StreamServerInit(vmNumber, ml)
	// 4. Streaming
	StreamProcessing(st)
}
