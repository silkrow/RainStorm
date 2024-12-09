package main

import (
	"RainStormServer/failuredetector"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	REP_NUM          = 3
	MAX_SERVER       = 10
	HTTP_PORT        = "4444"
	FILE_PATH_PREFIX = "../files/server/"
	MOVE_TIMEOUT     = time.Second
	MERGE_TIMEOUT    = 10 * time.Second
)

type File struct {
	filename string // Gives the path to local file on the server
	Mutex    *sync.RWMutex
	cache    map[time.Time]string
}

func NewFile(filename string) *File {
	return &File{
		filename: filename,
		Mutex:    &sync.RWMutex{},
		cache:    make(map[time.Time]string),
	}
}

type FileServer struct {
	aliveml            *failuredetector.MembershipList
	pred_list          []int
	succ_list          []int
	p_files            map[string]File
	r_files            map[string]File
	id                 int
	online             bool
	Mutex              sync.RWMutex
	coord_create_queue map[string]int
	coord_append_queue map[string]int
	mv_p_to_r          map[string]time.Time
}

func FileServerInit(ml *failuredetector.MembershipList, id int) *FileServer {
	return &FileServer{
		// Fields that don't need lock protection
		id:        id,
		online:    false,                      // I assume a single flip doesn't need to be protected that much.
		mv_p_to_r: make(map[string]time.Time), // Since this field is never used by HTTP handler

		// Shared file lists
		p_files: make(map[string]File),
		r_files: make(map[string]File),

		// Shared membership
		aliveml:   ml,
		pred_list: make([]int, 0),
		succ_list: make([]int, 0),

		// Shared by multiple http handlers
		coord_create_queue: make(map[string]int),
		coord_append_queue: make(map[string]int),
	}
}

func hashKey(input string) int {
	// Create a new SHA256 hash
	hash := sha256.New()
	// Write the input string as bytes to the hash
	hash.Write([]byte(input))
	// Get the resulting hash as a byte slice
	hashedBytes := hash.Sum(nil)
	// Convert the hash bytes to a hexadecimal string
	hashString := hex.EncodeToString(hashedBytes)

	// Convert the hex string to a big.Int
	bigIntHash := new(big.Int)
	bigIntHash.SetString(hashString, 16)

	// Mod the big.Int by 1000 and add 1 to map to the range [1, 1000]
	result := bigIntHash.Mod(bigIntHash, big.NewInt(1000)).Int64() + 1

	return int(result)
}

func equalSlices(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func findSuccessors(owner int, membershipList []int, n int) []int {
	var successors []int
	listLength := len(membershipList)

	// Find the index of the owner in the membership list
	ownerIndex := len(membershipList)
	for i, node := range membershipList {
		if node == owner {
			ownerIndex = i
			break
		}
	}

	if listLength-1 < n {
		n = listLength - 1
	}

	for i := 1; i <= n; i++ {
		successorIndex := (ownerIndex + i) % listLength
		successor := membershipList[successorIndex]
		successors = append(successors, successor)
	}

	return successors
}

func findPredecessors(owner int, membershipList []int, n int) []int {
	var predecessors []int
	listLength := len(membershipList)

	// Find the index of the owner in the membership list
	ownerIndex := len(membershipList)
	for i, node := range membershipList {
		if node == owner {
			ownerIndex = i
			break
		}
	}

	if listLength-1 < n {
		n = listLength - 1
	}

	for i := 1; i <= n; i++ {
		predecessorIndex := (ownerIndex - i + listLength) % listLength
		predecessor := membershipList[predecessorIndex]
		predecessors = append(predecessors, predecessor)
	}

	return predecessors
}

func findServerByfileID(ids []int, fileID int) int {
	server_id := -1
	min := 1000
	for _, i := range ids {
		if (i*100+1000-fileID)%1000 < min {
			server_id = i
			min = (i*100 + 1000 - fileID) % 1000
		}
	}
	return server_id
}

func fileExistsinPrimary(fs *FileServer, filename string) bool {
	fs.Mutex.Lock()
	defer fs.Mutex.Unlock()
	_, exists := fs.p_files[filename]
	if exists {
		return true
	} else {
		return false
	}
}

func fileExistsinReplica(fs *FileServer, filename string) bool {
	fs.Mutex.Lock()
	defer fs.Mutex.Unlock()
	_, exists := fs.r_files[filename]
	if exists {
		return true
	} else {
		return false
	}
}

// Return a []int of int that appear in array2 but missing in array1
func newComers(array1, array2 []int) []int {
	diff := []int{}
	elements := make(map[int]bool)

	// Add all elements of array1 to a map for quick lookups
	for _, num := range array1 {
		elements[num] = true
	}

	// Check elements in array2 and add to result if not in array1
	for _, num := range array2 {
		if !elements[num] {
			diff = append(diff, num)
		}
	}

	return diff
}

func id_to_domain(id int) string {
	return "fa24-cs425-68" + fmt.Sprintf("%02d", id) + ".cs.illinois.edu"
}

// Maintenance Thread
func Maintenance(fs *FileServer) {
	for {
		// Update online=true only if all members are in the network.
		if !fs.online && len(fs.aliveml.Alive_Ids()) == MAX_SERVER {
			fs.online = true
		}

		if !fs.online {
			for _, i := range fs.aliveml.Alive_Ids() {
				url := fmt.Sprintf("http://%s:%s/online", id_to_domain(i), HTTP_PORT)
				req, err := http.NewRequest(http.MethodGet, url, nil)
				if err != nil {
					log.Println("Error in creation of http.NewRequest", err)
					continue
				}

				client := &http.Client{}
				resp, err := client.Do(req)

				if err != nil {
					// log.Println("Error in sending request of online check", err)
					continue
				}
				defer resp.Body.Close()

				body, _ := io.ReadAll(resp.Body)
				if string(body) == "Yes" {
					fs.online = true
					break
				}
			}

			time.Sleep(time.Second)
			continue
		}

		//-------------- Maintenance logic ---------------//
		updatePredList(fs)
		updateSuccList(fs)
		delayedMove(fs)
		automerge(fs)

		time.Sleep(50 * time.Millisecond)
	}
}

func updatePredList(fs *FileServer) {
	new_pred_list := findPredecessors(fs.id, fs.aliveml.Alive_Ids(), REP_NUM)
	old_pred_list := fs.pred_list

	if equalSlices(new_pred_list, old_pred_list) {
		return
	}

	fs.Mutex.Lock()
	fs.pred_list = new_pred_list
	fs.Mutex.Unlock()

	// For replication restore
	newPreds := newComers(old_pred_list[:], new_pred_list)
	for _, i := range newPreds {
		// Create a new request to the external server
		url := fmt.Sprintf("http://%s:%s/storedfilenames?ftype=p", id_to_domain(i), HTTP_PORT)
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			log.Println("Error in request creation (when calling http.NewRequest) ", err)
			continue
		}

		client := &http.Client{}
		resp, err := client.Do(req)

		// TODO: what if the new predecessor is busy?
		if err != nil {
			log.Println("Error in sending http request", err)
			continue
		}
		defer resp.Body.Close()

		var filenames []string

		if resp.StatusCode == http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			filenames = strings.Split(string(body), " ")
		}

		for _, filename := range filenames {
			if len(filename) == 0 {
				continue
			}
			url := fmt.Sprintf("http://%s:%s/getting?filename=%s&ftype=p", id_to_domain(i), HTTP_PORT, filename)
			req2, err := http.NewRequest(http.MethodGet, url, nil)
			if err != nil {
				log.Println("Error in request creation (when calling http.NewRequest) ", err)
				continue
			}

			client := &http.Client{}
			resp, err := client.Do(req2)
			if err != nil {
				log.Println("Failed when checking file ", filename, "'s existence", err)
				continue
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				log.Println("Rejected, file " + filename + " doesn't exist")
				continue
			}
			body, _ := io.ReadAll(resp.Body)

			// Open the file in append mode, or create it if it doesn't exist
			file, err := os.OpenFile(FILE_PATH_PREFIX+filename, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				log.Println("Failed to open or create file " + filename)
				continue
			}
			defer file.Close()

			// Write the received content to the file
			_, err = file.Write(body)
			if err != nil {
				log.Println("Failed to write content to file " + filename)
				continue
			}

			fs.Mutex.Lock()
			fs.r_files[filename] = *NewFile(filename)
			fs.Mutex.Unlock()
		}
	}

	// For primary restore
	movedFiles := make(map[string]bool)

	fs.Mutex.Lock()
	for k, f := range fs.r_files {
		if findServerByfileID(fs.aliveml.Alive_Ids(), hashKey(k)) == fs.id {
			movedFiles[k] = true
			fs.p_files[k] = f
			delete(fs.r_files, k)
		}
	}
	fs.Mutex.Unlock()

	// -> Now push the replicas that are moved from r_files to p_files
	fs.Mutex.Lock()
	succList := fs.succ_list
	fs.Mutex.Unlock()
	for k := range movedFiles {
		for _, i := range succList {
			fileContent, _ := os.ReadFile(FILE_PATH_PREFIX + k)
			url := fmt.Sprintf("http://%s:%s/creating?filename=%s&ftype=r", id_to_domain(i), HTTP_PORT, k)
			req, _ := http.NewRequest(http.MethodPut, url, bytes.NewReader(fileContent))

			// Send the request
			client := &http.Client{}
			_, err := client.Do(req)
			if err != nil {
				log.Println("Failed when pushing replicas with http request", err)
			}
		}
	}

	// For rejoin (move those in p_files that no longer belong to yourself to r_files)
	fs.Mutex.Lock()
	for k := range fs.p_files {
		if findServerByfileID(fs.aliveml.Alive_Ids(), hashKey(k)) != fs.id {
			_, exist := fs.mv_p_to_r[k]
			if !exist {
				fs.mv_p_to_r[k] = time.Now()
			}
		}
	}
	fs.Mutex.Unlock()
}

func updateSuccList(fs *FileServer) {
	new_succ_list := findSuccessors(fs.id, fs.aliveml.Alive_Ids(), REP_NUM)

	if equalSlices(new_succ_list, fs.succ_list) {
		return
	}

	fs.Mutex.Lock()
	fs.succ_list = new_succ_list
	fs.Mutex.Unlock()
}

func delayedMove(fs *FileServer) {
	var toDelete []string
	// -> Call creating to pass the file to its real owner.
	for k, t := range fs.mv_p_to_r {
		if time.Now().After(t.Add(MOVE_TIMEOUT)) {
			fs.Mutex.Lock()
			owner := findServerByfileID(fs.aliveml.Alive_Ids(), hashKey(k))
			fs.Mutex.Unlock()
			fileContent, _ := os.ReadFile(FILE_PATH_PREFIX + k)
			url := fmt.Sprintf("http://%s:%s/creating?filename=%s&ftype=p", id_to_domain(owner), HTTP_PORT, k)
			req, _ := http.NewRequest(http.MethodPut, url, bytes.NewReader(fileContent))

			// Send the request
			client := &http.Client{}
			_, err := client.Do(req)

			if err != nil {
				fmt.Println("Error in making creating of p_file "+k+" to owner", err)
			}
			fs.Mutex.Lock()
			fs.r_files[k] = fs.p_files[k]
			delete(fs.p_files, k)
			fs.Mutex.Unlock()
			toDelete = append(toDelete, k)
		}
	}

	for _, k := range toDelete {
		delete(fs.mv_p_to_r, k)
	}

	// -> Then remove those replicas that are no longer needed.
	fs.Mutex.Lock()
	for k := range fs.r_files {
		p_server := findServerByfileID(fs.aliveml.Alive_Ids(), hashKey(k))
		exist := false
		for _, v := range fs.pred_list {
			if v == p_server {
				exist = true
				break
			}
		}

		if p_server != fs.id && !exist {
			fmt.Println("Removing replica file " + k + " since " + strconv.Itoa(p_server) + " is not a predecessor.")
			delete(fs.r_files, k)
		}
	}
	fs.Mutex.Unlock()
}

func automerge(fs *FileServer) {
	fs.Mutex.Lock()
	current_p_files := fs.p_files
	current_r_files := fs.r_files
	fs.Mutex.Unlock()

	for _, f := range current_p_files {
		if len(f.cache) > 0 {
			timestamps := make([]time.Time, 0, len(f.cache))
			for t := range f.cache {
				timestamps = append(timestamps, t)
			}

			sort.Slice(timestamps, func(i, j int) bool {
				return timestamps[i].Before(timestamps[j])
			})

			if time.Now().After(timestamps[len(timestamps)-1].Add(MERGE_TIMEOUT)) {
				url := fmt.Sprintf("http://%s:%s/merging?filename=%s", id_to_domain(fs.id), HTTP_PORT, f.filename)
				req, _ := http.NewRequest(http.MethodGet, url, nil)

				// Send the request
				client := &http.Client{}
				client.Do(req)
			}
		}
	}

	for _, f := range current_r_files {
		if len(f.cache) > 0 {
			timestamps := make([]time.Time, 0, len(f.cache))
			for t := range f.cache {
				timestamps = append(timestamps, t)
			}

			sort.Slice(timestamps, func(i, j int) bool {
				return timestamps[i].Before(timestamps[j])
			})

			if time.Now().After(timestamps[len(timestamps)-1].Add(5 * time.Second)) {
				url := fmt.Sprintf("http://%s:%s/merging?filename=%s", id_to_domain(fs.id), HTTP_PORT, f.filename)
				req, _ := http.NewRequest(http.MethodGet, url, nil)

				// Send the request
				client := &http.Client{}
				client.Do(req)
			}
		}
	}
}

// ------------------------- HTTP Handler -------------------------//
// Function to start HTTP server
func HTTPServer(fs *FileServer) {

	for {
		if fs.online {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	http.HandleFunc("/", fs.httpHandleSlash)        // Handle slash request (used when client search coordinator servers)
	http.HandleFunc("/create", fs.httpHandleCreate) // Handle file creation requests
	http.HandleFunc("/creating", fs.httpHandleCreating)
	http.HandleFunc("/existfile", fs.httpHandleExistence)   // Handle file existence queries, return YES/NO
	http.HandleFunc("/membership", fs.httpHandleMembership) // Return ids of online servers
	http.HandleFunc("/online", fs.httpHandleOnline)         // Return YES/NO to indicate online/offline
	http.HandleFunc("/append", fs.httpHandleAppend)
	http.HandleFunc("/appending", fs.httpHandleAppending)
	http.HandleFunc("/get", fs.httpHandleGet)
	http.HandleFunc("/getfromreplica", fs.httpHandleGetfromreplica)
	http.HandleFunc("/getting", fs.httpHandleGetting)
	http.HandleFunc("/store", fs.httpHandleStore)
	http.HandleFunc("/storedfilenames", fs.httpHandleStoredfilenames)
	http.HandleFunc("/merging", fs.httpHandleMerging)
	http.HandleFunc("/merge", fs.httpHandleMerge)
	http.HandleFunc("/ls", fs.httpHandleLs)

	fmt.Println("Starting HTTP server on :" + HTTP_PORT)
	log.Fatal(http.ListenAndServe(":"+HTTP_PORT, nil))
}

// HTTP handler functions
func (fs *FileServer) httpHandleLs(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		filename := r.URL.Query().Get("filename")

		fid := hashKey(filename)
		fs.Mutex.Lock()
		p_id := findServerByfileID(fs.aliveml.Alive_Ids(), fid)
		fs.Mutex.Unlock()

		url := fmt.Sprintf("http://%s:%s/existfile?filename=%s&ftype=p", id_to_domain(p_id), HTTP_PORT, filename)
		req2, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			http.Error(w, "Failed when checking file existence", http.StatusInternalServerError)
			return
		}

		client := &http.Client{}
		resp, err := client.Do(req2)
		defer resp.Body.Close()

		existFlag := true
		if resp.StatusCode == http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			if string(body) == "NO" {
				existFlag = false
			}
		}

		if !existFlag {
			http.Error(w, "File doesn't exist on HyDFS", http.StatusInternalServerError)
			return
		}

		response_s := "VM addresses and ids storing the file:\n" + id_to_domain(p_id) + " " + strconv.Itoa(p_id) + "\n"
		fs.Mutex.Lock()
		succ := findSuccessors(p_id, fs.aliveml.Alive_Ids(), REP_NUM)
		fs.Mutex.Unlock()

		for _, i := range succ {
			response_s += id_to_domain(i) + " " + strconv.Itoa(i) + "\n"
		}

		response_s += "File id of " + filename + " is " + strconv.Itoa(fid)

		w.Write([]byte(response_s))
		return
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (fs *FileServer) httpHandleCreate(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req map[string]string
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		// Access file1 and file2 directly from the map
		_, localExists := req["local"]
		hydfs, hydfsExists := req["hydfs"]
		if !localExists || !hydfsExists {
			http.Error(w, "Missing localfilename or HyDFSfilename in request", http.StatusBadRequest)
			return
		}

		// Find out the primary server of the HyDFS file
		fileID := hashKey(hydfs)
		responsible_server_id := findServerByfileID(fs.aliveml.Alive_Ids(), fileID)
		if responsible_server_id == -1 {
			log.Println("Invalid findServerByfileID result in httpHandleCreate")
			http.Error(w, "Rejected due to server internal error", http.StatusBadRequest)
			return
		}

		// Check if allowed to create
		url := fmt.Sprintf("http://%s:%s/existfile?filename=%s&ftype=p", id_to_domain(responsible_server_id), HTTP_PORT, hydfs)
		req2, _ := http.NewRequest(http.MethodGet, url, nil)

		client := &http.Client{}
		resp, err := client.Do(req2)
		if err != nil {
			log.Println("Failed when checking file existence")
			http.Error(w, "Failed when checking file existence", http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()

		existFlag := true
		if resp.StatusCode == http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			if string(body) == "NO" {
				existFlag = false
			}
		}

		if !existFlag {
			// Write the request into a cache
			fs.Mutex.Lock()
			defer fs.Mutex.Unlock()

			fs.coord_create_queue[hydfs] = responsible_server_id

			fmt.Fprintf(w, "Authorized")
		} else {
			http.Error(w, "Rejected, file "+hydfs+" already exists", http.StatusBadRequest)
		}
		return
	case http.MethodPut:
		filename := r.URL.Query().Get("filename")
		if filename == "" {
			http.Error(w, "HyDFS Filename not specified", http.StatusBadRequest)
			return
		}

		// Read the file content from the request body
		fileContent, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read file content from request", http.StatusInternalServerError)
			return
		}

		fs.Mutex.Lock()
		responsible_server_id, exist := fs.coord_create_queue[filename]
		fs.Mutex.Unlock()

		if !exist {
			http.Error(w, "Invalid upload, file creation not allowed", http.StatusBadRequest)
			return
		}
		// Create a new request to the external server
		url := fmt.Sprintf("http://%s:%s/creating?filename=%s&ftype=p", id_to_domain(responsible_server_id), HTTP_PORT, filename)
		req, _ := http.NewRequest(http.MethodPut, url, bytes.NewReader(fileContent))

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			log.Println("Failed to send creating request to primary owner server")
			http.Error(w, "Failed to send creating request to primary owner server", http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()

		// Check if the external server responded successfully
		if resp.StatusCode != http.StatusOK {
			log.Println("External server error in create http handler when sending creating request: " + resp.Status)
			http.Error(w, "External server error: "+resp.Status, resp.StatusCode)
			return
		}

		// Remove the task from queue
		fs.Mutex.Lock()
		delete(fs.coord_create_queue, filename)
		fs.Mutex.Unlock()
		fmt.Fprint(w, "File uploaded to external server "+id_to_domain(responsible_server_id)+" successfully")
		return
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (fs *FileServer) httpHandleSlash(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		w.Write([]byte{})
		return
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (fs *FileServer) httpHandleMembership(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		fs.Mutex.Lock()
		ids := fs.aliveml.Alive_Ids()
		fs.Mutex.Unlock()
		message := "No member in the file system??"
		if len(ids) != 0 {
			strs := make([]string, len(ids))
			for i, num := range ids {
				strs[i] = strconv.Itoa(num) // Convert each int to string
			}
			message = strings.Join(strs, ", ")
		}
		w.Write([]byte(message))
		return
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (fs *FileServer) httpHandleExistence(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		filename := r.URL.Query().Get("filename")
		ftype := r.URL.Query().Get("ftype")

		if ftype == "p" {
			if fileExistsinPrimary(fs, filename) {
				w.Write([]byte("YES"))
			} else {
				w.Write([]byte("NO"))
			}
		} else {
			if fileExistsinReplica(fs, filename) {
				w.Write([]byte("YES"))
			} else {
				w.Write([]byte("NO"))
			}
		}

		return
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (fs *FileServer) httpHandleOnline(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		fs.Mutex.Lock()
		defer fs.Mutex.Unlock()
		if fs.online {
			w.Write([]byte("Yes"))
		} else {
			w.Write([]byte("No"))
		}
		return
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (fs *FileServer) httpHandleAppending(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPut:
		// Get filename from query parameters
		filename := r.URL.Query().Get("filename")
		timeStampStr := r.URL.Query().Get("timestamp")
		initFlag := r.URL.Query().Get("init")

		timestamp, _ := time.Parse(time.RFC3339Nano, timeStampStr)
		if filename == "" {
			log.Println("HandleAppending Filename not specified")
			http.Error(w, "Filename not specified", http.StatusBadRequest)
			return
		}

		// Read the content from the request body
		content, err := io.ReadAll(r.Body)
		if err != nil {
			log.Println("HandleAppending Failed to read content from request body")
			http.Error(w, "Failed to read content from request body", http.StatusInternalServerError)
			return
		}

		if initFlag == "true" {
			fmt.Println("Appending")
			fmt.Println(string(content))
		}

		fs.Mutex.Lock()
		alive_ids := fs.aliveml.Alive_Ids()
		_, exist := fs.p_files[filename]
		if exist {
			fs.p_files[filename].Mutex.Lock()
			fs.p_files[filename].cache[timestamp] = string(content)
			fs.p_files[filename].Mutex.Unlock()
		} else {
			fs.r_files[filename].Mutex.Lock()
			fs.r_files[filename].cache[timestamp] = string(content)
			fs.r_files[filename].Mutex.Unlock()
		}
		fs.Mutex.Unlock()

		if initFlag == "true" {
			// Now broadcast the change
			p_server_id := findServerByfileID(alive_ids, hashKey(filename))
			reps := findSuccessors(p_server_id, alive_ids, REP_NUM)
			if p_server_id != fs.id {
				// Create a new request to the external server
				url := fmt.Sprintf("http://%s:%s/appending?filename=%s&timestamp=%s&init=false", id_to_domain(p_server_id), HTTP_PORT, filename, timestamp.Format(time.RFC3339Nano))
				req, _ := http.NewRequest(http.MethodPut, url, bytes.NewReader(content))

				// Send the request
				client := &http.Client{}
				client.Do(req)
			}
			for _, i := range reps {
				if i != fs.id {
					// Create a new request to the external server
					url := fmt.Sprintf("http://%s:%s/appending?filename=%s&timestamp=%s&init=false", id_to_domain(i), HTTP_PORT, filename, timestamp.Format(time.RFC3339Nano))
					req, _ := http.NewRequest(http.MethodPut, url, bytes.NewReader(content))

					// Send the request
					client := &http.Client{}
					client.Do(req)
				}
			}
		}

		fmt.Fprint(w, "File content appended successfully")
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (fs *FileServer) httpHandleCreating(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPut:
		// Get filename from query parameters
		filename := r.URL.Query().Get("filename")
		ftype := r.URL.Query().Get("ftype")
		if filename == "" {
			http.Error(w, "Filename not specified", http.StatusBadRequest)
			return
		}

		// Read the content from the request body
		content, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read content from request body", http.StatusInternalServerError)
			return
		}

		// Open the file in append mode, or create it if it doesn't exist
		file, err := os.OpenFile(FILE_PATH_PREFIX+filename, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			http.Error(w, "Failed to open or create file", http.StatusInternalServerError)
			return
		}
		defer file.Close()

		// Write the received content to the file
		_, err = file.Write(content)
		if err != nil {
			http.Error(w, "Failed to write content to file", http.StatusInternalServerError)
			return
		}

		fs.Mutex.Lock()
		if ftype == "p" {
			fs.p_files[filename] = *NewFile(filename)
		} else {
			fs.r_files[filename] = *NewFile(filename)
		}
		fs.Mutex.Unlock()

		// Pushing create to replicas
		if ftype == "p" {
			fs.Mutex.Lock()
			succ_list_temp := fs.succ_list
			fs.Mutex.Unlock()
			for _, i := range succ_list_temp {
				// Create a new request to the external server
				url := fmt.Sprintf("http://%s:%s/creating?filename=%s&ftype=r", id_to_domain(i), HTTP_PORT, filename)
				req, _ := http.NewRequest(http.MethodPut, url, bytes.NewReader(content))

				// Send the request
				client := &http.Client{}
				resp, err := client.Do(req)
				if err != nil {
					http.Error(w, "Failed to send creating request to external server", http.StatusInternalServerError)
					return
				}
				defer resp.Body.Close()
			}
		}

		fmt.Fprint(w, "File content created successfully")
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (fs *FileServer) httpHandleGet(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		var req map[string]string
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		// Access file1 and file2 directly from the map
		_, localExists := req["local"]
		hydfs, hydfsExists := req["hydfs"]
		if !localExists || !hydfsExists {
			http.Error(w, "Missing localfilename or HyDFSfilename in request", http.StatusBadRequest)
			return
		}

		// Find out the primary server of the HyDFS file
		fileID := hashKey(hydfs)
		responsible_server_id := findServerByfileID(fs.aliveml.Alive_Ids(), fileID)
		if responsible_server_id == -1 {
			log.Println("Invalid findServerByfileID result in httpHandleCreate")
			http.Error(w, "Rejected due to server internal error", http.StatusBadRequest)
			return
		}

		url := fmt.Sprintf("http://%s:%s/getting?filename=%s&ftype=p", id_to_domain(responsible_server_id), HTTP_PORT, hydfs)
		req2, _ := http.NewRequest(http.MethodGet, url, nil)

		client := &http.Client{Timeout: time.Minute * 2}
		resp, err := client.Do(req2)

		if err != nil {
			log.Println("Error in making getting requesting to external servers")
			http.Error(w, "Error fetching the file, internal error", http.StatusInternalServerError)
			return
		}

		if resp.StatusCode != http.StatusOK {
			http.Error(w, "Rejected, file "+hydfs+" doesn't exist", http.StatusBadRequest)
			return
		}
		// Stream the response body to the response writer directly
		w.Header().Set("Content-Type", "application/octet-stream")
		_, err = io.Copy(w, resp.Body)

		defer resp.Body.Close()
		if err != nil {
			log.Println("Error while sending file content:", err)
			http.Error(w, "Error sending file content", http.StatusInternalServerError)
		}
		return
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (fs *FileServer) httpHandleGetfromreplica(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		var req map[string]string
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		// Access file1 and file2 directly from the map
		_, localExists := req["local"]
		hydfs, hydfsExists := req["hydfs"]
		if !localExists || !hydfsExists {
			http.Error(w, "Missing localfilename or HyDFSfilename in request", http.StatusBadRequest)
			return
		}

		// Find out the primary server of the HyDFS file
		fileID := hashKey(hydfs)
		responsible_server_id := findServerByfileID(fs.aliveml.Alive_Ids(), fileID)
		if responsible_server_id == -1 {
			log.Println("Invalid findServerByfileID result in httpHandleCreate")
			http.Error(w, "Rejected due to server internal error", http.StatusBadRequest)
			return
		}

		url := fmt.Sprintf("%s:%s/getting?filename=%s&ftype=r", req["vm_add"], HTTP_PORT, hydfs)
		req2, _ := http.NewRequest(http.MethodGet, url, nil)

		client := &http.Client{Timeout: time.Minute * 2}
		resp, err := client.Do(req2)

		if err != nil {
			log.Println("Error in making getting requesting to external servers")
			http.Error(w, "Error fetching the file, internal error", http.StatusInternalServerError)
			return
		}

		if resp.StatusCode != http.StatusOK {
			http.Error(w, "Rejected, file "+hydfs+" doesn't exist", http.StatusBadRequest)
			return
		}
		// Stream the response body to the response writer directly
		w.Header().Set("Content-Type", "application/octet-stream")
		_, err = io.Copy(w, resp.Body)

		defer resp.Body.Close()
		if err != nil {
			log.Println("Error while sending file content:", err)
			http.Error(w, "Error sending file content", http.StatusInternalServerError)
		}
		return
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (fs *FileServer) httpHandleGetting(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		filename := r.URL.Query().Get("filename")
		ftype := r.URL.Query().Get("ftype")

		exist_flag := false
		fs.Mutex.Lock()
		if ftype == "p" {
			_, exist_flag = fs.p_files[filename]
		} else {
			_, exist_flag = fs.r_files[filename]
		}
		fs.Mutex.Unlock()

		if !exist_flag {
			http.Error(w, "Rejected, file "+filename+" doesn't exist", http.StatusBadRequest)
			return
		}

		file, err := os.Open(FILE_PATH_PREFIX + filename)
		if err != nil {
			http.Error(w, "Could not open file: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer file.Close()
		// fmt.Println("Reacting to get request for file " + filename)

		w.WriteHeader(http.StatusOK)
		if _, err := io.Copy(w, file); err != nil {
			http.Error(w, "Failed to send file: "+err.Error(), http.StatusInternalServerError)
		}

		return
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (fs *FileServer) httpHandleAppend(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req map[string]string
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		// Access file1 and file2 directly from the map
		_, localExists := req["local"]
		hydfs, hydfsExists := req["hydfs"]
		if !localExists || !hydfsExists {
			http.Error(w, "Missing localfilename or HyDFSfilename in request", http.StatusBadRequest)
			return
		}

		// Find out the primary server of the HyDFS file
		fileID := hashKey(hydfs)
		responsible_server_id := findServerByfileID(fs.aliveml.Alive_Ids(), fileID)
		if responsible_server_id == -1 {
			log.Println("Invalid findServerByfileID result in httpHandleAppend")
			http.Error(w, "Rejected due to server internal error", http.StatusBadRequest)
			return
		}

		// Check if allowed to append
		url := fmt.Sprintf("http://%s:%s/existfile?filename=%s&ftype=p", id_to_domain(responsible_server_id), HTTP_PORT, hydfs)
		req2, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			http.Error(w, "Failed when checking file existence", http.StatusInternalServerError)
			return
		}

		client := &http.Client{}
		resp, err := client.Do(req2)
		defer resp.Body.Close()

		existFlag := true
		if resp.StatusCode == http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			if string(body) == "NO" {
				existFlag = false
			}
		}

		if existFlag {
			// Write the request into a cache
			fs.Mutex.Lock()
			defer fs.Mutex.Unlock()

			fs.coord_append_queue[hydfs] = responsible_server_id

			fmt.Fprintf(w, "Authorized")
		} else {
			http.Error(w, "Rejected, file "+hydfs+" doesn't exist", http.StatusBadRequest)
		}
		return
	case http.MethodPut:
		filename := r.URL.Query().Get("filename")
		num := r.URL.Query().Get("num")
		if filename == "" {
			http.Error(w, "HyDFS Filename not specified", http.StatusBadRequest)
			return
		}

		// Read the file content from the request body
		fileContent, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read file content from request", http.StatusInternalServerError)
			return
		}

		fs.Mutex.Lock()
		responsible_server_id := findServerByfileID(fs.aliveml.Alive_Ids(), hashKey(filename))
		fs.Mutex.Unlock()

		if num == "1" {
			responsible_server_id = findSuccessors(responsible_server_id, fs.aliveml.Alive_Ids(), REP_NUM)[0]
		}

		if num == "2" {
			responsible_server_id = findSuccessors(responsible_server_id, fs.aliveml.Alive_Ids(), REP_NUM)[1]
		}

		if num == "3" {
			responsible_server_id = findSuccessors(responsible_server_id, fs.aliveml.Alive_Ids(), REP_NUM)[2]
		}

		// Create a new request to the external server
		url := fmt.Sprintf("http://%s:%s/appending?filename=%s&timestamp=%s&init=true", id_to_domain(responsible_server_id), HTTP_PORT, filename, time.Now().Format(time.RFC3339Nano))
		req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(fileContent))
		if err != nil {
			http.Error(w, "Failed to create request to external server", http.StatusInternalServerError)
			return
		}

		// Send the request
		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			http.Error(w, "Failed to send request to external server", http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()

		// Check if the external server responded successfully
		if resp.StatusCode != http.StatusOK {
			http.Error(w, "External server error: "+resp.Status, resp.StatusCode)
			return
		}

		// Remove the task from queue
		fs.Mutex.Lock()
		delete(fs.coord_append_queue, filename)
		fs.Mutex.Unlock()
		fmt.Fprint(w, "File uploaded to external server successfully")
		return
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (fs *FileServer) httpHandleStoredfilenames(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		ftype := r.URL.Query().Get("ftype")

		var file_list map[string]File
		fs.Mutex.Lock()
		if ftype == "p" {
			file_list = fs.p_files
		} else {
			file_list = fs.r_files
		}
		fs.Mutex.Unlock()

		keys := make([]string, 0, len(file_list))
		for key := range file_list {
			keys = append(keys, key)
		}

		filenameString := strings.Join(keys, " ")

		w.Write([]byte(filenameString))

		return
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (fs *FileServer) httpHandleStore(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		fs.Mutex.Lock()
		alive_ids := fs.aliveml.Alive_Ids()
		fs.Mutex.Unlock()

		response_string := ""

		for _, i := range alive_ids {
			response_string += "vm id " + strconv.Itoa(i) + ":\n"

			url := fmt.Sprintf("http://%s:%s/storedfilenames?ftype=p", id_to_domain(i), HTTP_PORT)
			req, err := http.NewRequest(http.MethodGet, url, nil)
			if err != nil {
				http.Error(w, "Failed when getting filenames", http.StatusInternalServerError)
				return
			}
			client := &http.Client{}
			resp, err := client.Do(req)
			defer resp.Body.Close()

			body, _ := io.ReadAll(resp.Body)
			response_string += "primaries: " + string(body) + "\n"

			url2 := fmt.Sprintf("http://%s:%s/storedfilenames?ftype=r", id_to_domain(i), HTTP_PORT)
			req2, err := http.NewRequest(http.MethodGet, url2, nil)
			if err != nil {
				http.Error(w, "Failed when getting filenames", http.StatusInternalServerError)
				return
			}
			client2 := &http.Client{}
			resp2, err := client2.Do(req2)
			defer resp2.Body.Close()

			body2, _ := io.ReadAll(resp2.Body)
			response_string += "replicas: " + string(body2) + "\n"
		}

		w.Write([]byte(response_string))

		return
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (fs *FileServer) httpHandleMerging(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		filename := r.URL.Query().Get("filename")

		var f *File

		fs.Mutex.Lock()
		if file, exist := fs.p_files[filename]; exist {
			f = &file
		} else if file, exist := fs.r_files[filename]; exist {
			f = &file
		}
		fs.Mutex.Unlock()

		if f != nil {
			if len(f.cache) == 0 {
				w.WriteHeader(http.StatusOK)
				return
			}

			// Acquire write lock since we'll clear the cache after merging
			f.Mutex.Lock()
			defer f.Mutex.Unlock()

			// Extract and sort timestamps
			timestamps := make([]time.Time, 0, len(f.cache))
			for t := range f.cache {
				timestamps = append(timestamps, t)
			}
			sort.Slice(timestamps, func(i, j int) bool {
				return timestamps[i].Before(timestamps[j])
			})

			// Open the file for appending
			file, err := os.OpenFile(FILE_PATH_PREFIX+f.filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
			if err != nil {
				log.Println("Merging failed, couldn't open file " + f.filename)
				http.Error(w, "Internal server error", http.StatusInternalServerError)
				return
			}
			defer file.Close()

			// Append contents in order of timestamps
			for _, t := range timestamps {
				content := f.cache[t]
				if _, err := file.WriteString(content); err != nil {
					log.Println("Merging failed, couldn't write to file " + f.filename)
					http.Error(w, "Internal server error", http.StatusInternalServerError)
					return
				}
				delete(f.cache, t)
			}

		} else {
			http.Error(w, "Rejected, file "+filename+" doesn't exist", http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusOK)
		return

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (fs *FileServer) httpHandleMerge(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		filename := r.URL.Query().Get("filename")

		fs.Mutex.Lock()
		alive_ids := fs.aliveml.Alive_Ids()
		fs.Mutex.Unlock()

		p_server := findServerByfileID(alive_ids, hashKey(filename))

		url := fmt.Sprintf("http://%s:%s/merging?filename=%s", id_to_domain(p_server), HTTP_PORT, filename)
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			http.Error(w, "Failed to create request to external server"+err.Error(), http.StatusInternalServerError)
			return
		}

		// Send the request
		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			http.Error(w, "Failed to send request to external server"+err.Error(), http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()

		// Check if the external server responded successfully
		if resp.StatusCode != http.StatusOK {
			http.Error(w, "External server error: "+resp.Status, resp.StatusCode)
			return
		}

		for _, i := range findSuccessors(p_server, alive_ids, REP_NUM) {
			url := fmt.Sprintf("http://%s:%s/merging?filename=%s", id_to_domain(i), HTTP_PORT, filename)
			req, err := http.NewRequest(http.MethodGet, url, nil)
			if err != nil {
				http.Error(w, "Failed to create request to external server"+err.Error(), http.StatusInternalServerError)
				return
			}

			// Send the request
			client := &http.Client{}
			resp, err := client.Do(req)
			if err != nil {
				http.Error(w, "Failed to send request to external server"+err.Error(), http.StatusInternalServerError)
				return
			}
			defer resp.Body.Close()

			// Check if the external server responded successfully
			if resp.StatusCode != http.StatusOK {
				http.Error(w, "External server error: "+resp.Status, resp.StatusCode)
				return
			}
		}

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}
