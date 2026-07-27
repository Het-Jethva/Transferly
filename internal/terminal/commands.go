package terminal

import (
	"strconv"
	"strings"

	"github.com/Het-Jethva/Transferly/internal/discovery"
)

func (a *App) handleLine(line string) bool {
	line = strings.TrimSpace(line)
	if line == "" {
		return false
	}
	if a.advanceControllableTime(line) {
		return false
	}
	a.noteTerminalActivity()
	if a.answerPendingConfirmation(line) || a.answerPendingOffer(line) {
		return false
	}

	command, argument := splitCommand(line)
	switch command {
	case "connect":
		if argument == "" || strings.ContainsAny(argument, " \t") {
			a.line("Usage: connect <peer-number|IPv4:port>")
			return false
		}
		a.connectTarget(argument)
	case "send":
		paths, ok := parsePathArguments(argument)
		if !ok {
			a.line("Usage: send <path>...")
			return false
		}
		a.sendPaths(paths)
	case "cancel":
		if argument != "" {
			a.line("Usage: cancel")
			return false
		}
		if !a.cancelActiveOffer() {
			a.line("There is no active Transfer Offer to cancel.")
		}
	case "disconnect":
		if argument != "" {
			a.line("Usage: disconnect")
			return false
		}
		a.disconnect()
	case "keep-alive":
		if argument != "" {
			a.line("Usage: keep-alive")
			return false
		}
		a.keepAlive()
	case "quit", "exit":
		return true
	case "help":
		a.printCommands()
	case "accept", "reject", "destination", "details", "cleanup-staging":
		a.line("There is no Transfer Offer awaiting approval.")
	default:
		a.line("Unknown command %q. Type help for available commands.", command)
	}
	return false
}

func splitCommand(line string) (string, string) {
	for index, character := range line {
		if character == ' ' || character == '\t' {
			return strings.ToLower(line[:index]), strings.TrimSpace(line[index+1:])
		}
	}
	return strings.ToLower(line), ""
}

func onePathArgument(argument string) (string, bool) {
	argument = strings.TrimSpace(argument)
	if len(argument) >= 2 && argument[0] == '"' && argument[len(argument)-1] == '"' {
		argument = argument[1 : len(argument)-1]
	} else if strings.ContainsAny(argument, "\"\r\n") {
		return "", false
	}
	return argument, argument != ""
}

func (a *App) printCommands() {
	a.line("Commands: connect <peer-number|IPv4:port>, send <path>..., cancel, keep-alive, disconnect, quit")
	a.line("  connect: select a discovered Available Peer or enter a directly reachable IPv4 endpoint; compare and confirm the six-digit code on both terminals.")
	a.line("  send: create a Transfer Offer from files and folders. The receiving Peer uses details, destination <path>, accept, or reject.")
	a.line("  cancel: cancel the active Transfer Offer without ending the verified Transfer Session. Ctrl+C has the same effect during transfer.")
	a.line("  keep-alive: deliberately reset the idle timeout. disconnect ends temporary trust; quit exits. Ctrl+C while idle disconnects and exits.")
}

func (a *App) connectTarget(target string) {
	if number, err := strconv.Atoi(target); err == nil {
		peers := []discovery.Peer(nil)
		if a.discovery != nil {
			peers = a.discovery.Peers()
		}
		if number < 1 || number > len(peers) {
			a.line("Available Peer %d is not currently listed. Use an IPv4:port endpoint instead.", number)
			return
		}
		peer := peers[number-1]
		a.line("Connecting to Available Peer %d at %s. Discovery names do not establish identity or trust.", number, peer.Endpoint())
		a.connect(peer.Endpoint())
		return
	}
	a.connect(target)
}

func (a *App) observeDiscovery() {
	for {
		select {
		case <-a.discovery.Changes():
			a.showAvailablePeers()
		case err := <-a.discovery.Errors():
			a.line("Discovery warning: %v", err)
		case <-a.rootContext.Done():
			return
		}
	}
}

func (a *App) showAvailablePeers() {
	peers := a.discovery.Peers()
	if len(peers) == 0 {
		a.line("No Available Peers discovered; use connect <IPv4:port> if multicast is unavailable.")
		return
	}
	a.line("Available Peers:")
	for index, peer := range peers {
		a.line("  [%d] %s at %s (untrusted discovery label)", index+1, peer.ComputerName, peer.Endpoint())
	}
}

func (a *App) setDiscoveryAvailable(available bool) {
	if a.discovery != nil {
		a.discovery.SetAvailable(available)
	}
}

func (a *App) answerPendingConfirmation(line string) bool {
	a.mu.Lock()
	current := a.current
	if current == nil || !current.waiting {
		a.mu.Unlock()
		return false
	}

	var answer bool
	switch strings.ToLower(line) {
	case "yes", "y":
		answer = true
	case "no", "n":
		answer = false
	default:
		a.mu.Unlock()
		a.line("Please type yes if the codes match, or no to close the connection.")
		return true
	}
	current.waiting = false
	answerChannel := current.answer
	a.mu.Unlock()

	select {
	case answerChannel <- answer:
	case <-current.context.Done():
	}
	return true
}

func (a *App) answerPendingOffer(line string) bool {
	a.mu.Lock()
	current := a.current
	if current == nil || current.incoming == nil || !current.incoming.waiting {
		a.mu.Unlock()
		return false
	}
	incoming := current.incoming
	a.mu.Unlock()

	command, argument := splitCommand(line)
	action := offerAction{}
	switch command {
	case "accept":
		if argument != "" {
			a.line("Usage: accept")
			return true
		}
		action.kind = "accept"
	case "reject":
		if argument != "" {
			a.line("Usage: reject")
			return true
		}
		action.kind = "reject"
	case "details":
		if argument != "" {
			a.line("Usage: details")
			return true
		}
		action.kind = "details"
	case "cleanup-staging":
		if argument != "" {
			a.line("Usage: cleanup-staging")
			return true
		}
		action.kind = "cleanup-staging"
	case "destination":
		path, ok := onePathArgument(argument)
		if !ok {
			a.line("Usage: destination <path>")
			return true
		}
		action.kind = "destination"
		action.destination = path
	case "send", "help", "keep-alive", "disconnect", "quit", "exit":
		return false
	default:
		a.line("Choose accept, reject, destination <path>, details, or cleanup-staging for this Transfer Offer.")
		return true
	}

	select {
	case incoming.actions <- action:
	case <-current.context.Done():
	}
	return true
}
