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
var (
	buildVersion = "dev" // Set for releases with -ldflags "-X main.buildVersion=vX.Y.Z".
	wireMajor    = "3"
	wireMinor    = "0"
)

func main() {
	flag.Usage = func() {
		output := flag.CommandLine.Output()
		fmt.Fprintln(output, "Transferly copies files directly between reachable Peers in a foreground terminal.")
		fmt.Fprintln(output, "")
		fmt.Fprintln(output, "Usage: transferly [options]")
		fmt.Fprintln(output, "")
		fmt.Fprintln(output, "Start Transferly on both computers to discover Available Peers, or manually use")
		fmt.Fprintln(output, "connect <peer-number|IPv4:port>, then compare the six-digit verification code on both")
		fmt.Fprintln(output, "terminals before confirming. In a verified Transfer Session use send <path>...;")
		fmt.Fprintln(output, "the receiving Peer can use details, destination <path>, accept, or reject.")
		fmt.Fprintln(output, "Use cancel for an active Transfer Offer, keep-alive for an idle session,")
		fmt.Fprintln(output, "disconnect to end temporary trust, and quit to exit. Ctrl+C cancels an active")
		fmt.Fprintln(output, "offer; otherwise it disconnects, cleans temporary state, and exits.")
		fmt.Fprintln(output, "")
		fmt.Fprintln(output, "Options (all overrides apply only to the current run):")
		fmt.Fprintln(output, "  --name <hint>  --output <folder>  --listen <IPv4:port>  --version")
		flag.PrintDefaults()
		fmt.Fprintln(output, "")
		fmt.Fprintln(output, "No configuration, identity, trust, history, logs, telemetry, relay, or update checks are created.")
		fmt.Fprintf(output, "Build: Transferly %s; wire protocol %s.%s. Updates are manual executable replacement.\n", buildVersion, wireMajor, wireMinor)
	}
	listenAddress := flag.String("listen", "0.0.0.0:0", "numeric IPv4 address and port (port 0 selects an available dynamic port)")
	computerName := flag.String("name", "", "temporary computer name advertised to Available Peers")
	defaultOutput := flag.String("output", "", "temporary default destination for incoming Transfer Offers")
	showVersion := flag.Bool("version", false, "print executable and wire-protocol versions, then exit")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "Transferly does not accept positional startup arguments. Use --help for options.")
		os.Exit(2)
	}

	major, majorError := strconv.Atoi(wireMajor)
	minor, minorError := strconv.Atoi(wireMinor)
	if majorError != nil || minorError != nil || major < 1 || minor < 0 {
		fmt.Fprintln(os.Stderr, "Transferly was built with an invalid wire protocol version.")
		os.Exit(1)
	}
	if *showVersion {
		fmt.Printf("Transferly %s\nWire protocol %d.%d\n", buildVersion, major, minor)
		return
	}
	faults, err := faultSettings()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Transferly was built with invalid fault-injection settings.")
		os.Exit(1)
	}
	application, err := terminal.New(terminal.Config{
		ListenAddress:      *listenAddress,
		Version:            session.Version{Major: major, Minor: minor},
		ProductVersion:     buildVersion,
		ComputerName:       *computerName,
		DefaultDestination: *defaultOutput,
		Faults:             faults,
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
