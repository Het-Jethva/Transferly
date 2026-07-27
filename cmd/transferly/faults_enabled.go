//go:build transferly_faults

package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/Het-Jethva/Transferly/internal/terminal"
)

// These variables exist only in a -tags transferly_faults build and are set
// with -ldflags -X by the process-level integration suite. They are never
// readable as command-line options, so a fault build still cannot be steered
// by whoever runs the executable.
var (
	corruptDigest    = "false"
	streamChunkDelay = "0s"
	controllableTime = "false"
)

func faultSettings() (terminal.FaultConfig, error) {
	corrupt, err := strconv.ParseBool(corruptDigest)
	if err != nil {
		return terminal.FaultConfig{}, fmt.Errorf("parse corruptDigest: %w", err)
	}
	chunkDelay, err := time.ParseDuration(streamChunkDelay)
	if err != nil {
		return terminal.FaultConfig{}, fmt.Errorf("parse streamChunkDelay: %w", err)
	}
	manualTime, err := strconv.ParseBool(controllableTime)
	if err != nil {
		return terminal.FaultConfig{}, fmt.Errorf("parse controllableTime: %w", err)
	}
	return terminal.FaultConfig{
		CorruptDigest:    corrupt,
		StreamChunkDelay: chunkDelay,
		ControllableTime: manualTime,
	}, nil
}
