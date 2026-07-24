package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"

	"github.com/Het-Jethva/Transferly/internal/session"
	"github.com/Het-Jethva/Transferly/internal/terminal"
)

// wireMajor is a string so release builds can set it with -ldflags -X while
// keeping wire-protocol versioning independent from the executable version.
var wireMajor = "1"

func main() {
	listenAddress := flag.String("listen", "0.0.0.0:0", "numeric IPv4 address and port to listen on (port 0 selects an available port)")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "Transferly does not accept positional startup arguments. Use --help for options.")
		os.Exit(2)
	}

	major, err := strconv.Atoi(wireMajor)
	if err != nil || major < 1 {
		fmt.Fprintln(os.Stderr, "Transferly was built with an invalid wire protocol version.")
		os.Exit(1)
	}
	application, err := terminal.New(terminal.Config{
		ListenAddress: *listenAddress,
		Version:       session.Version{Major: major, Minor: 0},
	}, os.Stdout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not start Transferly: %v\n", err)
		os.Exit(1)
	}
	if err := application.Run(os.Stdin); err != nil {
		fmt.Fprintf(os.Stderr, "Transferly stopped after a terminal error: %v\n", err)
		os.Exit(1)
	}
}
