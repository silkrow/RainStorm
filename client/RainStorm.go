package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"time"
)

func sendDataStream(serverAddress string) error {
	// Establish a TCP connection to the server
	conn, err := net.Dial("tcp", serverAddress)
	if err != nil {
		return fmt.Errorf("failed to connect to server: %w", err)
	}
	defer conn.Close()

	// Loop to send 1000 key-value pairs
	lines := []string{
		"This is the first line.",
		"Here is the second line.",
		"The third line is here.",
		"This is line number four.",
		"Line five comes next.",
		"Here is line six.",
		"Now we have line seven.",
		"This is the eighth line.",
		"Line nine is right here.",
		"Finally, the tenth line.",
	}

	for i, line := range lines {
		key := fmt.Sprintf("Line%d", i+1)
		value := line
		data := fmt.Sprintf("%s, %s\n", key, value)

		// Write the data to the connection
		_, err := conn.Write([]byte(data))
		if err != nil {
			return fmt.Errorf("failed to send data stream: %w", err)
		}
		fmt.Printf("Sent: %s", data)
	}

	return nil
}

func main() {
	registerWorker("http://fa24-cs425-6802.cs.illinois.edu:4445/register", "split")
	// registerWorker("http://fa24-cs425-6802.cs.illinois.edu:4445/register", "count")
	registerWorker("http://fa24-cs425-6803.cs.illinois.edu:4445/register", "count")
	registerWorker("http://fa24-cs425-6804.cs.illinois.edu:4445/register", "count")
	time.Sleep(50 * time.Millisecond)
	sendDataStream("fa24-cs425-6802.cs.illinois.edu:5555")
}

func registerWorker(workerURL string, exe_name string) {
	// Read the binary executable file
	executablePath := "../test_exe/" + exe_name // Replace with the path to your executable
	executable, err := os.ReadFile(executablePath)
	if err != nil {
		fmt.Printf("Failed to read executable: %v\n", err)
		return
	}

	// Base64 encode the executable
	encodedExecutable := base64.StdEncoding.EncodeToString(executable)

	// Additional JSON info
	info := map[string]string{
		"id":   "0",
		"name": exe_name,
	}

	// Create the request payload
	payload := struct {
		Executable string            `json:"executable"`
		Info       map[string]string `json:"info"`
	}{
		Executable: encodedExecutable,
		Info:       info,
	}

	// Encode payload to JSON
	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		fmt.Printf("Failed to encode payload: %v\n", err)
		return
	}

	// Create and send the HTTP POST request
	resp, err := http.Post(workerURL, "application/json", bytes.NewBuffer(jsonPayload))
	if err != nil {
		fmt.Printf("Failed to send request: %v\n", err)
		return
	}
	defer resp.Body.Close()

	// Handle the response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("Failed to read response: %v\n", err)
		return
	}

	// Print response status and body
	fmt.Printf("Response Status: %s\n", resp.Status)
	fmt.Printf("Response Body: %s\n", string(body))
}
