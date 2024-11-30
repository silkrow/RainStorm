package failuredetector

import (
	"fmt"
	"log"
	"net"
	"time"
)

type Sender struct {
	targetAddr string
	localAddr  string
	ackChannel chan bool
}

// NewSender initializes the local address and ackChannel
func NewSender(target string, port string, localAddr string) *Sender {
	return &Sender{
		targetAddr: target + ":" + port,
		localAddr:  localAddr,
		ackChannel: make(chan bool),
	}
}

func (s *Sender) Ping(ddl time.Duration) error {
	udpAddr, err := net.ResolveUDPAddr("udp", s.targetAddr)
	if err != nil {
		return fmt.Errorf("Error (Ping) parsing target address: %v", err)
	}
	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return fmt.Errorf("Error (Ping) dialing target address: %v", err)
	}

	defer conn.Close()

	_, err = conn.Write([]byte(fmt.Sprintf("PING from %s", s.localAddr)))
	if err != nil {
		return fmt.Errorf("Error sending Ping: %v", err)
	}

	buffer := make([]byte, 1024)

	conn.SetReadDeadline(time.Now().Add(ddl))

	n, _, err := conn.ReadFromUDP(buffer)
	if err != nil {
		if err, ok := err.(net.Error); ok && err.Timeout() {
			return fmt.Errorf("Ping timed out waiting for response")
		}
		return fmt.Errorf("Error reading response: %v", err)
	}

	response := string(buffer[:n])
	log.Printf("Received response: %s", response)

	return nil
}

func (s *Sender) Reping(ddl time.Duration, repingaddr string) error {
	udpAddr, err := net.ResolveUDPAddr("udp", s.targetAddr)
	if err != nil {
		return fmt.Errorf("Error (RePing) parsing target address: %v", err)
	}
	conn, err := net.DialUDP("udp", nil, udpAddr)

	if err != nil {
		return fmt.Errorf("Error (RePing) dialing target address: %v", err)
	}

	defer conn.Close()

	_, err = conn.Write([]byte(fmt.Sprintf("REPING from %s to %s", s.localAddr, repingaddr)))
	if err != nil {
		return fmt.Errorf("Error sending RePing request: %v", err)
	}

	buffer := make([]byte, 1024)

	conn.SetReadDeadline(time.Now().Add(ddl))

	n, _, err := conn.ReadFromUDP(buffer)
	if err != nil {
		if err, ok := err.(net.Error); ok && err.Timeout() {
			return fmt.Errorf("RePing timed out waiting for response")
		}
		return fmt.Errorf("Error reading reping response: %v", err)
	}

	response := string(buffer[:n])
	log.Printf("Received response: %s", response)

	return nil
}

func (s *Sender) Gossip(timeStamp time.Time, topicaddr string, state string, source string, inc int) error {
	udpAddr, err := net.ResolveUDPAddr("udp", s.targetAddr)
	if err != nil {
		return fmt.Errorf("Error (Gossiping) parsing target address: %v", err)
	}
	conn, err := net.DialUDP("udp", nil, udpAddr)

	if err != nil {
		return fmt.Errorf("Error (Gossiping) dialing target address: %v", err)
	}

	defer conn.Close()

	_, err = conn.Write([]byte(fmt.Sprintf("GOSSIP from %s passed by %s update %s %s incNum %d timestamp %s", source, s.localAddr, topicaddr, state, inc, timeStamp.Format(time.RFC3339))))
	if err != nil {
		return fmt.Errorf("Error sending Gossip: %v", err)
	}

	currentTime := time.Now()

	if state == "JOIN" && s.localAddr == topicaddr && currentTime.Sub(timeStamp) < 1*time.Second {
		buffer := make([]byte, 2048)

		conn.SetReadDeadline(time.Now().Add(5 * time.Second))

		n, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			if err, ok := err.(net.Error); ok && err.Timeout() {
				return fmt.Errorf("Join gossip timed out waiting for response")
			}
			return fmt.Errorf("Error reading join gossip response: %v", err)
		}

		response := string(buffer[:n])
		return fmt.Errorf(response)
	}

	return nil
}

func (s *Sender) Cmd(cmd string) error {
	udpAddr, err := net.ResolveUDPAddr("udp", s.targetAddr)
	if err != nil {
		return fmt.Errorf("Error (Cmd) parsing target address: %v", err)
	}
	conn, err := net.DialUDP("udp", nil, udpAddr)

	if err != nil {
		return fmt.Errorf("Error (Cmd) dialing target address: %v", err)
	}

	defer conn.Close()

	_, err = conn.Write([]byte(fmt.Sprintf(cmd)))
	if err != nil {
		return fmt.Errorf("Error sending Cmd: %v", err)
	}

	return nil
}
