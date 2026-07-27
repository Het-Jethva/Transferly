//go:build !transferly_faults

package terminal

import (
	"context"
	"io"
)

// FaultConfig carries no settings in a release build. Fault injection exists
// only to let process-level integration tests drive protocol and timing
// boundaries that are otherwise unreachable from the public terminal
// interface, so the shipped executable must not contain those branches at all.
// Build with -tags transferly_faults to compile the injectable variant.
type FaultConfig struct{}

func (FaultConfig) validate() error { return nil }

func newSessionClock(FaultConfig) sessionClock { return realSessionClock{} }

func (a *App) faultDigest(digest string) string { return digest }

func (a *App) faultStreamReader(_ context.Context, source io.Reader) io.Reader { return source }

func (a *App) timeControlEnabled() bool { return false }

func (a *App) advanceControllableTime(string) bool { return false }
