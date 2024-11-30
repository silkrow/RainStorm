package failuredetector

import (
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Define states for each member
const (
	Alive     = "Alive"
	Failed    = "Failed"
	Suspected = "Suspected"
)

// Member represents a machine in the network
type Member struct {
	IP        string
	State     string
	Timestamp time.Time
	incNum    int
}

// MembershipList stores the status of all members and a mutex for synchronization
type MembershipList struct {
	Members map[string]Member // map[domain]Member
	mu      sync.Mutex        // mutex to protect Members map
}

// NewMembershipList creates a new membership list
func NewMembershipList() *MembershipList {
	return &MembershipList{
		Members: make(map[string]Member),
	}
}

// UpdateMember updates the state of an existing member or replaces it if the IP is already in use
func (ml *MembershipList) UpdateMember(domain string, state string, timeStamp time.Time, inc int) {
	ml.mu.Lock()         // Acquire the lock before modifying the map
	defer ml.mu.Unlock() // Ensure the lock is released after the operation

	// Check if the member with the given domain already exists
	if member, exists := ml.Members[domain]; exists {
		// Update the existing member's state and timestamp
		if member.State == Alive && state == Suspected {
			fmt.Printf("Suspecion on failure of %s at %s original timeStamp: %s\n", domain, time.Now(), member.Timestamp)
		}

		// if member.State != Failed && state == Failed {
		// 	fmt.Printf("Failure of %s at %s original timeStamp: %s\n", domain, time.Now(), member.Timestamp)
		// }

		member.State = state
		member.Timestamp = timeStamp
		member.incNum = inc
		ml.Members[domain] = member
	} else {
		log.Printf("Member with domain %s not found\n", domain)
	}
}

// AddMember adds a new member or updates an existing one in the membership list
func (ml *MembershipList) AddMember(domain string, state string, inc int) {
	ml.mu.Lock()         // Acquire the lock before modifying the map
	defer ml.mu.Unlock() // Ensure the lock is released after the operation

	// First, check if a member with the same IP exists
	for existingDomain := range ml.Members {
		if existingDomain == domain {
			// If it exists, update it instead of adding a new member
			ml.UpdateMember(existingDomain, state, time.Now(), inc)
			return
		}
	}

	// If no existing member is found, add the new member
	member := Member{
		IP:        domain,
		State:     state,
		Timestamp: time.Now(),
		incNum:    inc,
	}
	ml.Members[domain] = member
}

// GetMember returns the details of a member by domain name
func (ml *MembershipList) GetMember(domain string) (Member, bool) {
	ml.mu.Lock()         // Acquire the lock before reading the map
	defer ml.mu.Unlock() // Ensure the lock is released after the operation

	member, exists := ml.Members[domain]
	return member, exists
}

func (ml *MembershipList) GetIncNumber(domain string) int {
	ml.mu.Lock()         // Acquire the lock before reading the map
	defer ml.mu.Unlock() // Ensure the lock is released after the operation

	member, exists := ml.Members[domain]
	if !exists {
		return 0
	}
	return member.incNum
}

// RemoveMember removes a member from the list
func (ml *MembershipList) RemoveMember(domain string) {
	ml.mu.Lock()         // Acquire the lock before modifying the map
	defer ml.mu.Unlock() // Ensure the lock is released after the operation

	delete(ml.Members, domain)
}

// Display the membership list
func (ml *MembershipList) Display() {
	ml.mu.Lock()         // Acquire the lock before reading the map
	defer ml.mu.Unlock() // Ensure the lock is released after the operation

	fmt.Println("Membership List:")
	for _, member := range ml.Members {
		if member.State == Failed {
			continue
		}
		fmt.Printf("Domain: %s, State: %s, IncNumber: %d, TimeStamp: %s\n", member.IP, member.State, member.incNum, member.Timestamp)
	}
}

// RandomMember returns a random member excluding the member with the given IP
func (ml *MembershipList) RandomMember(memberIP string) *Member {
	ml.mu.Lock()         // Acquire the lock before reading the map
	defer ml.mu.Unlock() // Ensure the lock is released after the operation

	if len(ml.Members) == 0 {
		return nil
	}
	var aliveMembers []Member
	for _, member := range ml.Members {
		if (member.State == Alive || member.State == Suspected) && member.IP != memberIP {
			aliveMembers = append(aliveMembers, member)
		}
	}

	if len(aliveMembers) == 0 {
		return nil
	}

	index := rand.Intn(len(aliveMembers))
	return &aliveMembers[index] // Return a pointer to the randomly selected member
}

func (ml *MembershipList) GetRandomMembers(k int, excludeIPs []string) []*Member {
	ml.mu.Lock()         // Acquire the lock before reading the map
	defer ml.mu.Unlock() // Ensure the lock is released after the operation

	var aliveMembers []Member // Store copies of the members
	for _, member := range ml.Members {
		if (member.State == Alive || member.State == Suspected) && !contains(excludeIPs, member.IP) {
			aliveMembers = append(aliveMembers, member) // Append a copy of the member
		}
	}

	if k > len(aliveMembers) {
		k = len(aliveMembers)
	}

	rand.Shuffle(len(aliveMembers), func(i, j int) {
		aliveMembers[i], aliveMembers[j] = aliveMembers[j], aliveMembers[i]
	})

	// Convert to slice of pointers before returning
	result := make([]*Member, k)
	for i := 0; i < k; i++ {
		result[i] = &aliveMembers[i] // Convert to pointers
	}

	return result
}

// contains checks if a slice contains a specific string
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// GetMemberTimestamp returns the timestamp of a member by domain name
func (ml *MembershipList) GetMemberTimestamp(domain string) (time.Time, bool) {
	ml.mu.Lock()         // Acquire the lock before reading the map
	defer ml.mu.Unlock() // Ensure the lock is released after the operation

	member, exists := ml.Members[domain]
	if !exists {
		return time.Time{}, false // Return zero value of time.Time and false if the member doesn't exist
	}
	return member.Timestamp, true // Return the timestamp and true if the member exists
}

// Stringfy iterates through the membership list and returns a single-line string
func (ml *MembershipList) Stringfy() string {
	ml.mu.Lock()         // Acquire the lock before reading the map
	defer ml.mu.Unlock() // Ensure the lock is released after the operation

	var members []string
	for domain, member := range ml.Members {
		memberStr := fmt.Sprintf("%s;%s;%s;%d", domain, member.State, member.Timestamp.Format(time.RFC3339), member.incNum)
		members = append(members, memberStr)
	}

	return strings.Join(members, ",")
}

// Parse takes the string generated by Stringfy and recreates the membership list
func (ml *MembershipList) Parse(membersStr string) error {
	ml.mu.Lock()         // Acquire the lock before modifying the map
	defer ml.mu.Unlock() // Ensure the lock is released after the operation

	ml.Members = make(map[string]Member) // Reset the membership list

	members := strings.Split(membersStr, ",")
	for _, memberStr := range members {
		parts := strings.Split(memberStr, ";")
		if len(parts) != 4 {
			return fmt.Errorf("invalid member format: %s", memberStr)
		}

		domain := parts[0]
		state := parts[1]
		timestamp, err := time.Parse(time.RFC3339, parts[2])
		if err != nil {
			return fmt.Errorf("invalid timestamp for member %s: %v", domain, err)
		}

		incNum, err := strconv.Atoi(parts[3])
		if err != nil {
			return fmt.Errorf("invalid incNum for member %s: %v", domain, err)
		}

		ml.Members[domain] = Member{
			IP:        domain,
			State:     state,
			Timestamp: timestamp,
			incNum:    incNum,
		}
	}

	return nil
}

// Clear removes all members from the membership list
func (ml *MembershipList) Clear() {
	ml.mu.Lock()         // Acquire the lock before modifying the map
	defer ml.mu.Unlock() // Ensure the lock is released after the operation

	ml.Members = make(map[string]Member) // Reset the map to an empty one
}

func (ml *MembershipList) CheckMemberStatus(domain string) (string, bool) {
	member, exists := ml.GetMember(domain)
	if exists {
		return member.State, true
	}
	return "", false // Member does not exist
}

func (ml *MembershipList) Alive_Ids() []int {
	ml.mu.Lock()         // Acquire the lock before reading the map
	defer ml.mu.Unlock() // Ensure the lock is released after the operation

	// Pre-allocate the slice with an initial capacity equal to the number of members
	ids := make([]int, 0, len(ml.Members))

	for _, member := range ml.Members {
		if member.State == Failed {
			continue
		}

		// Extract IP and split the string
		s := member.IP
		parts := strings.Split(s, "-")

		// Ensure the IP has enough parts and the expected format
		if len(parts) >= 3 && len(parts[2]) >= 4 {
			twoDigitStr := parts[2][2:4] // Extract the "68" part as a string

			// Attempt to convert the extracted string into an integer
			if id, err := strconv.Atoi(twoDigitStr); err == nil {
				ids = append(ids, id) // Collect the integer
			} else {
				// Log error if conversion fails, but continue processing
				log.Printf("Error parsing IP part: %s, error: %v", twoDigitStr, err)
			}
		}
	}

	// Sort the ids in ascending order
	sort.Ints(ids)
	return ids
}
