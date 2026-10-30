// Command pagurus-term is conch's child while a member holds the office:
// it hands the term to the pagurus agent over the agent's term socket. The
// agent passes it to `conch elect`; it is not for operators. It links only
// the standard library (see internal/termshim).
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/alien43/pagurus/internal/termshim"
)

var version = "dev"

func main() {
	if len(os.Args) == 2 && os.Args[1] == "version" {
		fmt.Println(version)
		return
	}
	sock := flag.String("socket", "", "the agent's term socket")
	flag.Parse()
	os.Exit(termshim.Run(*sock))
}
