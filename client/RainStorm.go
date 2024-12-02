package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
)

const (
	EXE_FOLDER = "../test_exe/"
)

func main() {

	if len(os.Args) != 6 {
		fmt.Println("Usage: RainStorm op1 op2 f1 f2 num")
		os.Exit(1)
	}

	op1 := os.Args[1]
	op2 := os.Args[2]
	f1 := os.Args[3]
	f2 := os.Args[4]
	num := os.Args[5]

	_, err := strconv.Atoi(num)
	if err != nil {
		fmt.Println("Invalid value for 'num': must be an integer")
		os.Exit(1)
	}

	// Prepare the request payload
	info := map[string]string{
		"op1": op1,
		"op2": op2,
		"f1":  f1,
		"f2":  f2,
		"num": num,
	}

	op1_path := EXE_FOLDER + op1
	exe1, err := os.ReadFile(op1_path)
	if err != nil {
		fmt.Printf("Failed to read executable: %v\n", err)
		return
	}
	encoded_exe1 := base64.StdEncoding.EncodeToString(exe1)

	op2_path := EXE_FOLDER + op2
	exe2, err := os.ReadFile(op2_path)
	if err != nil {
		fmt.Printf("Failed to read executable: %v\n", err)
		return
	}
	encoded_exe2 := base64.StdEncoding.EncodeToString(exe2)

	// Create the request payload
	payload := struct {
		Exe1 string            `json:"exe1"`
		Exe2 string            `json:"exe2"`
		Info map[string]string `json:"info"`
	}{
		Exe1: encoded_exe1,
		Exe2: encoded_exe2,
		Info: info,
	}

	// Encode payload to JSON
	jsonPayload, err := json.Marshal(payload)

	if err != nil {
		fmt.Println("Error creating request payload:", err)
		os.Exit(1)
	}

	// Send HTTP POST request
	resp, err := http.Post("http://fa24-cs425-6801.cs.illinois.edu:4445/rainstorm", "application/json", bytes.NewReader(jsonPayload))
	if err != nil {
		fmt.Println("Error sending request:", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	// Handle response
	if resp.StatusCode == http.StatusOK {
		fmt.Println("Stream processing completed successfully")
	} else {
		fmt.Printf("Error: %s\n", resp.Status)
	}
}
