package failuredetector

import (
	"fmt"
	"log"
	"strings"
	"time"
)

const (
	N              = 10              // Number of machines
	K              = 3               // Number of random machines to ask for a ping
	Timeout        = 2 * time.Second // Timeout for receiving an ACK
	RepingTimeout  = 2 * time.Second
	PingPort       = "2234"
	RepingPort     = "2235"
	GossipPort     = "2236"
	CmdPort        = "2237" // Used in HyDFS to listen for commands from clients
	G              = 4
	GossipDuration = 3 * time.Second
	IntroducerAddr = "fa24-cs425-6801.cs.illinois.edu"
	FD_period      = time.Second
)

func Failuredetect(ml *MembershipList, vmNumber int) {
	// Construct the domain name based on the VM number
	domain := "fa24-cs425-68" + fmt.Sprintf("%02d", vmNumber) + ".cs.illinois.edu"

	// ml := NewMembershipList()

	// Failure detection go routains
	go startListenPing(domain, ml)
	go startListenPingRequest(domain, ml)
	go startListenGossiping(domain, ml)
	go startListenCmd(domain, ml)
	go startFailureDetect(ml, domain)

	time.Sleep(500 * time.Millisecond)

	// Sent join request automatically
	joinFD(ml, domain)

	for {
		time.Sleep(1 * time.Second)
	}
}

func startListenPing(myDomain string, ml *MembershipList) {
	r := NewReceiver(myDomain, PingPort)
	go r.Listen(ml)
}

func startListenPingRequest(myDomain string, ml *MembershipList) {
	r := NewReceiver(myDomain, RepingPort)
	go r.Listen(ml)
}

func startListenGossiping(myDomain string, ml *MembershipList) {
	r := NewReceiver(myDomain, GossipPort)
	go r.Listen(ml)
}

func startListenCmd(myDomain string, ml *MembershipList) {
	r := NewReceiver(myDomain, CmdPort)
	go r.Listen(ml)
}

func startFailureDetect(ml *MembershipList, myDomain string) {
	for {
		if len(ml.Members) == 0 { // Haven't join the network yet
			time.Sleep(FD_period)
			continue
		}

		// Randomly select a member to ping
		member := ml.RandomMember(myDomain)
		if member == nil {
			// log.Println("No members available to ping.")
			time.Sleep(FD_period)
			continue
		}

		// log.Printf("Pinging %s...\n", member.IP)

		// Create a sender for the selected member
		s := NewSender(member.IP, PingPort, myDomain)
		err := s.Ping(Timeout)
		if err != nil {
			log.Printf("Ping to %s failed: %s\n", member.IP, err)
			kMembers := ml.GetRandomMembers(K, []string{myDomain, member.IP})
			ackReceived := false

			// log.Println("Checking live status of " + member.IP + " with")

			for _, kMember := range kMembers {
				// log.Println(i, kMember.IP)
				kSender := NewSender(kMember.IP, RepingPort, myDomain)
				if err := kSender.Reping(RepingTimeout, member.IP); err == nil {
					ackReceived = true
					break
				}
			}

			if !ackReceived {
				// log.Printf("Marking %s as failed.\n", member.IP)
				updatedState := Failed
				gossipCmd := "FAILED"

				ml.UpdateMember(member.IP, updatedState, time.Now(), ml.GetIncNumber(member.IP))

				gMembers := ml.GetRandomMembers(G, []string{myDomain, member.IP})
				log.Printf("Gossiping failure/suspect of %s with: \n", member.IP)
				// log.Printf("Failure detection/suspicion of %s at %s\n", member.IP, time.Now())
				for _, gMember := range gMembers {
					// log.Println(i, gMember.IP)
					gSender := NewSender(gMember.IP, GossipPort, myDomain)
					if err := gSender.Gossip(time.Now(), member.IP, gossipCmd, myDomain, ml.GetIncNumber(member.IP)); err != nil {
						// log.Printf("Failed to send gossip to %s.\n", gMember.IP)
					}
				}
			}
		}

		time.Sleep(FD_period) // Wait before the next ping
	}
}

func joinFD(ml *MembershipList, domain string) {
	s := NewSender(IntroducerAddr, GossipPort, domain)
	err := s.Ping(10 * time.Second)
	if err != nil {
		fmt.Println("Failed to join, introducer offline")
	} else {
		err := s.Gossip(time.Now(), domain, "JOIN", domain, 0)
		if err != nil {
			feedback := err.Error()
			if strings.HasPrefix(feedback, "APPROVED") {
				fmt.Println("Joined failure detection network!")
				var copyMembership string
				fmt.Sscanf(feedback, "APPROVED %s", &copyMembership)
				ml.Parse(copyMembership)
			} else {
				fmt.Println("Failed to join, introducer not in network")
			}
		}
	}
}
