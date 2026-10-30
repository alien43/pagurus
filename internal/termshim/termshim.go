// Package termshim is conch's child for a term: it hands conch's term
// (CONCH_REV, CONCH_LEASE) to the agent over the agent's term socket and
// exits with the code the agent gives back. It imports only the standard
// library, so the process (one per leading stack) stays small: the
// pagurus binary's package init()s (grpc, etcd, zap, incus) cost ~3 MB of
// anon each.
package termshim

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"
)

// Hello is what the shim sends: conch's view of the term it won.
type Hello struct {
	Rev   int64  `json:"rev"`
	Lease string `json:"lease"` // hex, as CONCH_LEASE
	PID   int    `json:"pid"`
}

// Bye is the agent's answer when the term ends.
type Bye struct {
	Exit int `json:"exit"`
}

// Run is `pagurus-term --socket PATH`. It exits 64 when not run by conch
// (CONCH_REV, CONCH_LEASE unset), 69 when the agent's socket does not
// answer, 70 when conch dies or the agent's reply is bad, and otherwise
// with the agent's exit code.
func Run(socket string) int {
	rev, err := strconv.ParseInt(os.Getenv("CONCH_REV"), 10, 64)
	if err != nil || os.Getenv("CONCH_LEASE") == "" {
		fmt.Fprintln(os.Stderr, "pagurus term: run by conch elect (CONCH_REV, CONCH_LEASE unset)")
		return 64
	}
	c, err := net.Dial("unix", socket)
	if err != nil {
		fmt.Fprintln(os.Stderr, "pagurus term:", err)
		return 69
	}
	defer c.Close()
	b, _ := json.Marshal(Hello{Rev: rev, Lease: os.Getenv("CONCH_LEASE"), PID: os.Getpid()})
	if _, err := c.Write(append(b, '\n')); err != nil {
		return 69
	}
	parent := os.Getppid()
	go func() { // conch gone (killed outright): drop the term
		for range time.Tick(500 * time.Millisecond) {
			if os.Getppid() != parent {
				os.Exit(70)
			}
		}
	}()
	line, err := bufio.NewReader(c).ReadBytes('\n')
	var bye Bye
	if err != nil || json.Unmarshal(line, &bye) != nil {
		return 70
	}
	return bye.Exit
}
