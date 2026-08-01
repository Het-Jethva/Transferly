//go:build !transferly_faults

package main

import "github.com/Het-Jethva/Transferly/internal/terminal"

// faultSettings returns an empty configuration. A default build carries no
// fault-injection variables, so there is nothing to parse and no way to enable
// injected behavior at run time.
func faultSettings() (terminal.FaultConfig, error) {
	return terminal.FaultConfig{}, nil
}
