package main

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

const (
	TYPE_LEADER = 0
	TYPE_WORKER = 1
	WORKER_PORT = "4445"
	TCP_IN_PORT = "5555"
)

type Task struct {
	ID       int
	exe      string
	parentID int
	childID  int
}

type Data struct {
	ID    int
	Key   string
	Value string
}

type StreamServer struct {
	stype     int
	locallist []int
	taskmap   map[int]Task
	dataQueue chan Data
	stdinPipe io.WriteCloser
}

func StreamServerInit(id int) *StreamServer {
	var stype int
	if id == 1 {
		stype = TYPE_LEADER
	} else {
		stype = TYPE_WORKER
	}
	return &StreamServer{
		stype:     stype,
		locallist: make([]int, 0),
		taskmap:   make(map[int]Task),
		dataQueue: make(chan Data, 10),
	}
}

func StreamProcessing(st *StreamServer) {
	go workerHTTPServer(st)
	go tcpServer(st)

	select {}
}

func workerHTTPServer(st *StreamServer) {
	http.HandleFunc("/register", st.httpHandleRegister)
	http.HandleFunc("/rainstorm", st.httpHandleRainStorm)
	fmt.Println("Stream server on port " + WORKER_PORT)
	log.Fatal(http.ListenAndServe(":"+WORKER_PORT, nil))
}

func (st *StreamServer) httpHandleRainStorm(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		if st.stype != TYPE_LEADER {
			http.Error(w, "Not the leader", http.StatusMethodNotAllowed)
			return
		}

		// Parse request body
		var request struct {
			Exe1 string            `json:"exe1"`
			Exe2 string            `json:"exe2"`
			Info map[string]string `json:"info"`
		}

		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		// Validate info map
		op1, op2, f1, f2, num := request.Info["op1"], request.Info["op2"], request.Info["f1"], request.Info["f2"], request.Info["num"]
		if op1 == "" || op2 == "" || f1 == "" || f2 == "" || num == "" {
			http.Error(w, "Missing input parameters", http.StatusBadRequest)
			return
		}

		// Convert num to integer
		numVal, err := strconv.Atoi(num)
		if err != nil || numVal <= 0 {
			http.Error(w, "Invalid value for 'num'", http.StatusBadRequest)
			return
		}

		// Decode executables
		op1File, err := decodeAndSaveExecutable(request.Exe1, "op1_executable")
		if err != nil {
			http.Error(w, "Failed to process executable op1: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer os.Remove(op1File) // Clean up temp file

		op2File, err := decodeAndSaveExecutable(request.Exe2, "op2_executable")
		if err != nil {
			http.Error(w, "Failed to process executable op2: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer os.Remove(op2File) // Clean up temp file

		registerWorker("http://fa24-cs425-6802.cs.illinois.edu:4445/register", op1, &request.Exe1)
		// registerWorker("http://fa24-cs425-6802.cs.illinois.edu:4445/register", "count")
		registerWorker("http://fa24-cs425-6803.cs.illinois.edu:4445/register", op2, &request.Exe2)
		registerWorker("http://fa24-cs425-6804.cs.illinois.edu:4445/register", op2, &request.Exe2)
		time.Sleep(50 * time.Millisecond)
		err = sendDataSource("fa24-cs425-6802.cs.illinois.edu:5555", f1)
		if err != nil {
			http.Error(w, "Failed to send source file: "+err.Error(), http.StatusInternalServerError)
			return
		}

		// Respond to client
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Stream processing completed successfully"))

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func sendDataSource(serverAddress string, filename string) error {

	parts := []string{"get", filename, "local_file.txt"}

	if parts[0] == "get" && len(parts) == 3 {
		hydfs := parts[1]
		local := parts[2]

		liveServer := "http://fa24-cs425-6801.cs.illinois.edu:" + HTTP_PORT
		if liveServer != "" {
			// Prepare the JSON payload
			reqData := map[string]string{
				"local": local,
				"hydfs": hydfs,
			}
			reqBody, err := json.Marshal(reqData)
			if err != nil {
				fmt.Println("Error marshalling request data:", err)
				return err
			}

			// Create the GET request with a body
			url := fmt.Sprintf("%s/get", liveServer)
			req, err := http.NewRequest(http.MethodGet, url, bytes.NewReader(reqBody))
			if err != nil {
				fmt.Println("Error creating request:", err)
				return err
			}
			req.Header.Set("Content-Type", "application/json")

			// Send the request
			client := &http.Client{}
			resp, err := client.Do(req)
			if err != nil {
				fmt.Println("Request to server failed:", err)
				return err
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				// Read the response body
				body, err := io.ReadAll(resp.Body)
				if err != nil {
					fmt.Println("Error reading response body:", err)
					return err
				}

				// Write the content to the file
				filePath := local
				err = os.WriteFile(filePath, body, 0644)
				if err != nil {
					fmt.Println("Error writing to file:", err)
					return err
				}

				fmt.Println("File get successfully!")
			} else {
				body, _ := io.ReadAll(resp.Body)
				fmt.Println("Get file failed:", string(body))
			}
		} else {
			fmt.Println("No live servers available")
		}
	}

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

// Helper function to decode base64 and save as a file
func decodeAndSaveExecutable(encoded, filename string) (string, error) {
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return "", fmt.Errorf("failed to decode base64: %v", err)
	}

	tmpFile, err := os.CreateTemp("", filename)
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %v", err)
	}
	defer tmpFile.Close()

	if _, err := tmpFile.Write(data); err != nil {
		return "", fmt.Errorf("failed to write to temp file: %v", err)
	}

	if err := os.Chmod(tmpFile.Name(), 0755); err != nil {
		return "", fmt.Errorf("failed to set executable permissions: %v", err)
	}

	return tmpFile.Name(), nil
}

func registerWorker(workerURL string, exe_name string, exe_content *string) {

	encodedExecutable := *exe_content

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

func (st *StreamServer) httpHandleRegister(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req struct {
			Executable string            `json:"executable"` // Changed to string
			Info       map[string]string `json:"info"`
		}

		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		// Decode Base64-encoded executable
		executable, err := base64.StdEncoding.DecodeString(req.Executable)
		if err != nil {
			http.Error(w, "Invalid Base64 encoding in executable", http.StatusBadRequest)
			return
		}

		// Validate required fields
		if len(executable) == 0 || req.Info == nil {
			http.Error(w, "Missing required fields", http.StatusBadRequest)
			return
		}

		// Handle the received executable and JSON info
		err = st.processRegistration(executable, req.Info)
		if err != nil {
			http.Error(w, "Failed to process registration", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Registration successful"))

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (st *StreamServer) processRegistration(executable []byte, info map[string]string) error {
	// Save the executable to a file
	err := os.WriteFile(info["name"], executable, 0700)
	if err != nil {
		return fmt.Errorf("failed to save executable: %w", err)
	}

	// Launch a goroutine to execute the binary
	go func(executablePath string) {
		cmd := exec.Command("./" + executablePath)

		// Set up input to the process
		stdin, err := cmd.StdinPipe()
		if err != nil {
			fmt.Printf("Failed to create stdin pipe: %v\n", err)
			return
		}
		st.stdinPipe = stdin

		// Set up output from the process
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			fmt.Printf("Failed to create stdout pipe: %v\n", err)
			return
		}

		stderr, err := cmd.StderrPipe()
		if err != nil {
			fmt.Printf("Failed to create stderr pipe: %v\n", err)
			return
		}

		// Start the process
		if err := cmd.Start(); err != nil {
			fmt.Printf("Failed to start executable: %v\n", err)
			return
		}

		// Redirect output in a separate goroutine
		go redirectOutput(info["name"], stdout, stderr)

		// Wait for the process to complete
		if err := cmd.Wait(); err != nil {
			fmt.Printf("Executable finished with error: %v\n", err)
		} else {
			fmt.Printf("Executable finished successfully.\n")
		}

		st.stdinPipe = nil // Clear the stdinPipe once the process completes
	}(info["name"])

	// Store the task in the taskmap
	newTask := createTask(info["name"])
	st.taskmap[0] = newTask

	return nil
}

// redirectOutput processes the stdout and stderr of the executable
// and sends it to another server's TCP based on certain conditions.
func redirectOutput(exe_name string, stdout, stderr io.ReadCloser) {
	// Create readers for stdout and stderr
	stdoutReader := bufio.NewReader(stdout)
	stderrReader := bufio.NewReader(stderr)

	// Redirect stdout and process the output
	go func() {
		for {
			line, err := stdoutReader.ReadString('\n')
			if err != nil {
				if err != io.EOF {
					fmt.Printf("Error reading stdout: %v\n", err)
				}
				break
			}

			fmt.Printf(line)
			// Process stdout line here (e.g., if check)
			if strings.Contains(line, "i") {
				// Send to the first server if the condition is met
				if exe_name == "split" {
					sendToTCPServer(line, "fa24-cs425-6803.cs.illinois.edu:5555")
				}
			} else {
				// Send to the second server otherwise
				if exe_name == "split" {
					sendToTCPServer(line, "fa24-cs425-6804.cs.illinois.edu:5555")
				}
			}
		}
	}()

	// Redirect stderr and process the output (optional)
	go func() {
		for {
			line, err := stderrReader.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					// Process finished, exit the loop gracefully
					break
				}
				fmt.Printf("Error reading stderr: %v\n", err)
				break
			}

			fmt.Println(line)
		}
	}()
}

// sendToTCPServer sends the output to a specified TCP server
func sendToTCPServer(output, serverAddress string) {
	// Format the output as needed (e.g., adding a new line)
	message := fmt.Sprintf("%s", output)

	// Establish a connection to the server
	conn, err := net.Dial("tcp", serverAddress)
	if err != nil {
		fmt.Printf("Failed to connect to server %s: %v\n", serverAddress, err)
		return
	}
	defer conn.Close()

	// Send the processed message to the server
	_, err = conn.Write([]byte(message))
	if err != nil {
		fmt.Printf("Failed to send message to server: %v\n", err)
		return
	}
}

func createTask(name string) Task {
	return Task{
		ID:       0,
		parentID: 0,
		childID:  0,
		exe:      name,
	}
}

func tcpServer(st *StreamServer) {
	listener, err := net.Listen("tcp", ":"+TCP_IN_PORT)
	if err != nil {
		log.Fatalf("Failed to start TCP server: %v\n", err)
	}
	defer listener.Close()

	fmt.Printf("TCP server listening on port %s\n", TCP_IN_PORT)
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Failed to accept connection: %v\n", err)
			continue
		}

		go st.handleTCPConnection(conn)
	}
}

func (st *StreamServer) handleTCPConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	for {
		// Read data from the TCP connection
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				fmt.Printf("Error reading TCP connection: %v\n", err)
			}
			break
		}

		line = strings.TrimSpace(line)

		// Feed data to the running executable
		if st.stdinPipe != nil {
			_, err := st.stdinPipe.Write([]byte(fmt.Sprintf("%s\n", line)))
			if err != nil {
				fmt.Printf("Failed to write to executable stdin: %v\n", err)
			}
		} else {
			fmt.Printf("No executable running to handle data: %v\n", line)
		}
	}
}
