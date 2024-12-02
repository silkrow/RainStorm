package main

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
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

func workerHTTPServer(st *StreamServer) {
	http.HandleFunc("/register", st.httpHandleRegister)
	fmt.Println("Worker server on")
	log.Fatal(http.ListenAndServe(":"+WORKER_PORT, nil))
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

func StreamProcessing(st *StreamServer) {

	go workerHTTPServer(st)
	go tcpServer(st)

	select {}
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
