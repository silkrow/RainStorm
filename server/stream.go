package main

import (
	"RainStormServer/failuredetector"
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
	RESULT_TIME = 30
	WORKER_PORT = "4445"
	TCP_IN_PORT = "5555"
)

type Data struct {
	ID    int
	Key   string
	Value string
}

type StreamServer struct {
	stype     int
	ml        *failuredetector.MembershipList
	dataQueue chan Data
	stdinPipe io.WriteCloser
	destFile  string

	// Monitoring and dataflow
	task1_workers []int
	task2_workers []int
	emit_table    []int
	exe_name      string
	buffile       *os.File
	start_ts      time.Time
}

func StreamServerInit(id int, ml *failuredetector.MembershipList) *StreamServer {
	var stype int
	if id == 1 {
		stype = TYPE_LEADER
	} else {
		stype = TYPE_WORKER
	}
	return &StreamServer{
		stype:     stype,
		ml:        ml,
		dataQueue: make(chan Data, 10),
		destFile:  "",
	}
}

func StreamProcessing(st *StreamServer) {
	st.buffile, _ = os.OpenFile("tmp.txt", os.O_TRUNC|os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	os.Truncate("tmp.txt", 0)
	defer st.buffile.Close()
	go streamHTTPServer(st)
	go tcpServer(st)
	if st.stype == TYPE_LEADER {
		go writeDest(st)
	}

	select {}
}

func streamHTTPServer(st *StreamServer) {
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

		st.destFile = f2

		// Get alive machine IDs
		aliveIDs := st.ml.Alive_Ids()
		if len(aliveIDs) < 2*numVal+1 {
			http.Error(w, "Not enough alive machines to register executables", http.StatusInternalServerError)
			return
		}

		// Register exe2 on the next lowest 3 IDs
		task2Machines := aliveIDs[numVal+1 : 2*numVal+1]
		for _, id := range task2Machines {
			registerWorker(fmt.Sprintf("http://fa24-cs425-68%02d.cs.illinois.edu:%s/register", id, WORKER_PORT), op2, &request.Exe2, []int{1})
		}
		st.task2_workers = task2Machines // Store the task2 machines

		// Register exe1 on the lowest 3 IDs
		task1Machines := aliveIDs[1 : numVal+1]
		for _, id := range task1Machines {
			registerWorker(fmt.Sprintf("http://fa24-cs425-68%02d.cs.illinois.edu:%s/register", id, WORKER_PORT), op1, &request.Exe1, task2Machines)
		}
		st.task1_workers = task1Machines // Store the task1 machines

		time.Sleep(50 * time.Millisecond)

		// Send the partitioned data source
		err = sendPartitionedDataSource(task1Machines, f1, numVal)
		if err != nil {
			http.Error(w, "Failed to send source file: "+err.Error(), http.StatusInternalServerError)
			return
		}

		st.start_ts = time.Now()

		// Respond to client
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Stream processing completed successfully"))

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func sendPartitionedDataSource(servers []int, filename string, numServers int) error {

	parts := []string{"get", filename, "local_file.txt"}
	hydfs := parts[1]
	local := parts[2]

	liveServer := "http://fa24-cs425-6801.cs.illinois.edu:" + HTTP_PORT
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

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		fmt.Println("Failed to fetch file:", string(body))
		return fmt.Errorf("failed to fetch file, status: %s", resp.Status)
	}

	// Read the response body (file content)
	fileContent, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error reading file content:", err)
		return err
	}

	// Step 2: Partition the file content for distribution
	lines := strings.Split(string(fileContent), "\n")

	// Partition the lines evenly
	partitions := make([][]string, numServers)
	for i, line := range lines {
		partitions[i%numServers] = append(partitions[i%numServers], line)
	}

	// Step 3: Send each partition to the corresponding server
	for i, serverId := range servers {
		partitionData := strings.Join(partitions[i], "\n")

		conn, err := net.Dial("tcp", id_to_domain(serverId)+":"+TCP_IN_PORT)
		if err != nil {
			fmt.Printf("Failed to connect to server %s: %v\n", id_to_domain(serverId), err)
			return err
		}
		defer conn.Close()

		// Write the partition data to the server
		partitionData = partitionData + "\n"
		_, err = conn.Write([]byte(partitionData))
		if err != nil {
			fmt.Printf("Failed to send data to server %s: %v\n", id_to_domain(serverId), err)
			return err
		}
		fmt.Printf("Sent partition to server %s successfully\n", id_to_domain(serverId))
	}

	fmt.Println("File partitioning and distribution completed successfully!")
	return nil
}

func registerWorker(workerURL string, exe_name string, exe_content *string, emit_table []int) {

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
		EmitTable  []int             `json:"emit_table"`
	}{
		Executable: encodedExecutable,
		Info:       info,
		EmitTable:  emit_table, // Add emit_table here
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
			EmitTable  []int             `json:"emit_table"`
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

		st.emit_table = req.EmitTable

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
		defer st.stdinPipe.Close()

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
		go st.redirectOutput(info["name"], stdout, stderr)

		// Wait for the process to complete
		if err := cmd.Wait(); err != nil {
			fmt.Printf("Executable finished with error: %v\n", err)
		} else {
			fmt.Printf("Executable finished successfully.\n")
		}

		st.stdinPipe = nil // Clear the stdinPipe once the process completes
	}(info["name"])

	return nil
}

// redirectOutput processes the stdout and stderr of the executable
// and sends it to another server's TCP based on certain conditions.
func (st *StreamServer) redirectOutput(exe_name string, stdout, stderr io.ReadCloser) {
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

			// Redirect
			key := strings.SplitN(line, ",", 2)[0]
			sendToTCPServer(line, id_to_domain(st.emit_table[hashKey(key)%len(st.emit_table)])+":"+TCP_IN_PORT)
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

	// Create a channel to pass data to stdinPipe
	dataChannel := make(chan string)

	// Goroutine to read from the TCP connection
	go func() {
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
			// Send data to the stdinPipe goroutine via the channel
			dataChannel <- line
		}
		close(dataChannel) // Close the channel when done reading
	}()

	// Goroutine to handle feeding data to the stdinPipe
	go func() {
		for line := range dataChannel {
			if st.stdinPipe != nil {
				_, err := st.stdinPipe.Write([]byte(fmt.Sprintf("%s\n", line)))
				if err != nil {
					fmt.Printf("Failed to write to executable stdin: %v\n", err)
				}
			} else {
				if st.stype == TYPE_LEADER {

					// Write the received content to the file
					_, err := st.buffile.WriteString(fmt.Sprintf("%s\n", line))
					if err != nil {
						log.Println("Failed to write content to file")
						return
					}
				}
			}
		}
	}()

	// This will let the main function exit when the goroutines are done
	select {}
}

func writeDest(st *StreamServer) {
	for {
		time.Sleep(50 * time.Millisecond)
		if st.destFile == "" {
			continue
		}
		if st.start_ts.IsZero() {
			continue
		} else {
			if time.Now().After(st.start_ts.Add(RESULT_TIME * time.Second)) {
				_, err := st.buffile.Seek(0, 0)
				if err != nil {
					log.Println("Moving File Pointer failed:", err)
					return
				}

				b, err := io.ReadAll(st.buffile)
				if err != nil {
					log.Println("Reading File failed:", err)
					return
				}

				liveServer := "http://" + id_to_domain(1) + ":" + HTTP_PORT

				// Step 1: Request authorization to create the file
				data := fmt.Sprintf(`{"local":"%s","hydfs":"%s"}`, "emptyFile", st.destFile)
				authResponse, err := http.Post(liveServer+"/create", "application/json", bytes.NewBuffer([]byte(data)))
				if err != nil {
					log.Println("Request to server failed:", err)
					return
				}
				defer authResponse.Body.Close()

				if authResponse.StatusCode == http.StatusOK {

					req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/create?filename=%s", liveServer, st.destFile), bytes.NewReader(b))
					if err != nil {
						log.Println("Failed to create request:", err)
						return
					}

					client := &http.Client{}
					uploadResponse, err := client.Do(req)
					if err != nil {
						log.Println("File upload failed:", err)
						return
					}
					defer uploadResponse.Body.Close()

					if uploadResponse.StatusCode != http.StatusOK {
						body, _ := io.ReadAll(uploadResponse.Body)
						log.Printf("File upload failed: %s\n", body)
					}

					log.Println("now " + time.Now().String() + " " + "timestamp " + st.start_ts.Format(time.RFC3339) + " " + string(b))
				} else {
					body, _ := io.ReadAll(authResponse.Body)
					log.Printf("Authorization failed: %s\n", body)
					return
				}
				return
			}
		}
	}
}
