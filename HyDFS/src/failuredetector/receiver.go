package failuredetector

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"
)

type GossipBuffer struct {
	mu     sync.Mutex
	buffer map[string]int
}

// NewGossipBuffer initializes the gossip buffer
func NewGossipBuffer() *GossipBuffer {
	return &GossipBuffer{
		buffer: make(map[string]int),
	}
}

// AddGossip increments the count for the gossip message
func (gb *GossipBuffer) AddGossip(gossip string) int {
	gb.mu.Lock()
	defer gb.mu.Unlock()
	gb.buffer[gossip]++
	return gb.buffer[gossip]
}

type Receiver struct {
	localhost    string
	myaddress    string // The address of the sender
	port         string
	gossipBuffer *GossipBuffer // Buffer for tracking gossip messages
}

var dropRate = 0.0

// NewReceiver creates a new receiver with the specified address
func NewReceiver(myaddr string, port string) *Receiver {
	return &Receiver{
		localhost:    "0.0.0.0",
		myaddress:    myaddr,
		port:         port,
		gossipBuffer: NewGossipBuffer(),
	}
}

// Listen starts the UDP server to listen for incoming messages
func (r *Receiver) Listen(ml *MembershipList) {
	addr, err := net.ResolveUDPAddr("udp", r.localhost+":"+r.port)
	if err != nil {
		log.Fatal("Error (Listen) resolving UDP address:", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatal("Error (Listen) starting UDP server:", err)
	}
	defer conn.Close()

	log.Println("Receiver listening on", addr.String())

	for {
		buffer := make([]byte, 1024)
		n, senderAddr, err := conn.ReadFromUDP(buffer)
		if err != nil {
			log.Println("Error (Listen) reading from UDP:", err)
			continue
		}

		message := string(buffer[:n])

		randomValue := rand.Float64()
		if randomValue < dropRate && !strings.HasPrefix(message, "SUS") {
			message = ""
		}

		// Print the bandwidth
		// fmt.Println(len(message), time.Now())

		if len(ml.Members) > 0 || r.myaddress == IntroducerAddr || strings.HasPrefix(message, "SUS") { // Only handle the requests if the node is in the network/ is the introducer
			if strings.HasPrefix(message, "PING") {
				var senderLocalAddr string
				_, err := fmt.Sscanf(message, "PING from %s", &senderLocalAddr)
				if err == nil {
					// log.Printf("Ping received from %s", senderLocalAddr)
					conn.WriteToUDP([]byte(fmt.Sprintf("ACK from %s", r.myaddress)), senderAddr)
				} else {
					log.Println("Failed to parse sender address:", err)
				}
			} else if strings.HasPrefix(message, "REPING") {
				var targetAddr string
				var requestAddr string
				_, err := fmt.Sscanf(message, "REPING from %s to %s", &requestAddr, &targetAddr)
				if err == nil {
					log.Printf("Reping Request from %s to ping %s received", requestAddr, targetAddr)
					s := NewSender(targetAddr, PingPort, r.myaddress)
					err = s.Ping(3 * time.Second)
					if err != nil {
						log.Printf("Ping to %s failed: %s\n", targetAddr, err)
					} else {
						conn.WriteToUDP([]byte(fmt.Sprintf("ACK: PING to %s received", targetAddr)), senderAddr)
					}
				} else {
					log.Println("Failed to parse target address:", err)
				}
			} else if strings.HasPrefix(message, "GOSSIP") {
				var topicAddr string
				var requestAddr string
				var state string
				var inc int
				var passerAddr string
				var timeStamp string
				_, err := fmt.Sscanf(message, "GOSSIP from %s passed by %s update %s %s incNum %d timestamp %s", &requestAddr, &passerAddr, &topicAddr, &state, &inc, &timeStamp)
				if err == nil {
					log.Printf("Gossip from %s received", passerAddr)
					gossipKey := fmt.Sprintf("%s:%s:%s:%s", requestAddr, topicAddr, state, timeStamp)
					gossipCount := r.gossipBuffer.AddGossip(gossipKey)
					if gossipCount > 3 {
						log.Printf("Gossip from %s about %s received %d times, ignoring.", requestAddr, topicAddr, gossipCount)
						continue
					}

					memberState, exists := ml.CheckMemberStatus(topicAddr)

					parsedTime, _ := time.Parse(time.RFC3339, timeStamp)
					switch state {
					case "FAILED":
						if exists && memberState != Failed {
							ml.UpdateMember(topicAddr, Failed, parsedTime, ml.GetIncNumber(topicAddr)) // Failed, we don't actually care about the incNum
							log.Printf("Failure detection of %s at %s\n", topicAddr, time.Now())
						}
					case "SUSPECTED":
						if exists && memberState != Failed {

							// Check if it is me
							if topicAddr == r.myaddress {
								ml.UpdateMember(r.myaddress, Alive, time.Now(), ml.GetIncNumber(r.myaddress)+1) // Increase my incNumber
								// Pass this alive messages to others rather than the suspecion message
								inc = ml.GetIncNumber(r.myaddress)
								state = "ALIVE"
							} else {
								if memberState == Alive {
									if inc >= ml.GetIncNumber(topicAddr) {
										ml.UpdateMember(topicAddr, Suspected, parsedTime, inc)
										log.Printf("Failure suspicion of %s at %s\n", topicAddr, time.Now())
									} else {
										state = "ALIVE"
										inc = ml.GetIncNumber(topicAddr)
										tstamp, _ := ml.GetMemberTimestamp(topicAddr)
										timeStamp = tstamp.String()
									}
								} else {
									if inc > ml.GetIncNumber(topicAddr) {
										ml.UpdateMember(topicAddr, Suspected, parsedTime, inc)
									} else {
										inc = ml.GetIncNumber(topicAddr)
										tstamp, _ := ml.GetMemberTimestamp(topicAddr)
										timeStamp = tstamp.String()
									}
								}
							}
						}
					case "ALIVE":
						if exists && memberState != Failed {
							if memberState == Suspected {
								if inc > ml.GetIncNumber(topicAddr) {
									ml.UpdateMember(topicAddr, Alive, parsedTime, inc)
									log.Printf("Canceling suspicion of %s at %s\n", topicAddr, time.Now())
								} else {
									state = "SUSPECTED"
									inc = ml.GetIncNumber(topicAddr)
									tstamp, _ := ml.GetMemberTimestamp(topicAddr)
									timeStamp = tstamp.String()
								}
							} else {
								if inc > ml.GetIncNumber(topicAddr) {
									ml.UpdateMember(topicAddr, Alive, parsedTime, inc)
								} else {
									inc = ml.GetIncNumber(topicAddr)
									tstamp, _ := ml.GetMemberTimestamp(topicAddr)
									timeStamp = tstamp.String()
								}
							}
						}
					case "JOIN":
						if r.myaddress == IntroducerAddr {
							if len(ml.Members) == 0 && topicAddr != r.myaddress {
								conn.WriteToUDP([]byte(fmt.Sprintf("REFUSED")), senderAddr)
								continue // Don't pass on the gossip
							} else { // Add the member
								ml.RemoveMember(topicAddr)
								ml.AddMember(topicAddr, Alive, 0) // Initializing, incNum is 0
								// Pass a copy of the membership list back to the new comer.
								copyMembership := ml.Stringfy()
								conn.WriteToUDP([]byte(fmt.Sprintf("APPROVED "+copyMembership)), senderAddr)
							}
						} else {
							ml.RemoveMember(topicAddr)
							ml.AddMember(topicAddr, Alive, 0) // Initializing, incNum is 0
						}
					}

					currentTime := time.Now()
					if currentTime.Sub(parsedTime) <= GossipDuration {
						excludeList := []string{r.myaddress, requestAddr, topicAddr}
						if state == "JOIN" {
							excludeList = append(excludeList, IntroducerAddr)
						}
						gMembers := ml.GetRandomMembers(G, excludeList)
						log.Printf("Passing on gossip of timestamp %s from %s about %s with: \n", timeStamp, requestAddr, topicAddr)
						for i, gMember := range gMembers {
							log.Println(i, gMember.IP)
							gSender := NewSender(gMember.IP, GossipPort, r.myaddress)
							if err := gSender.Gossip(parsedTime, topicAddr, state, requestAddr, inc); err != nil {
								log.Printf("Failed to send gossip to %s. With error: %s\n", gMember.IP, err.Error())
							}
						}
					}

				} else {
					log.Println("Failed to parse target address:", err)
				}
			} else {
				log.Println("Unknown message format:", message)
			}
		}
	}
}
