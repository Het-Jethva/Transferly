//go:build transferly_faults

package terminal

import (
	"context"
	"errors"
	"io"
	"time"
)

// FaultConfig enables deterministic protocol-boundary and timing faults for
// process-level integration tests. Release builds omit this file entirely, so
// none of these branches reach the shipped executable.
type FaultConfig struct {
	CorruptDigest    bool          // Send a wrong SHA-256 to exercise receiver rejection.
	StreamChunkDelay time.Duration // Slow each chunk read so transfers stay observably active.
	ControllableTime bool          // Replace the session clock and enable advance-time.
}

func (f FaultConfig) validate() error {
	if f.StreamChunkDelay < 0 {
		return errors.New("stream chunk delay cannot be negative")
	}
	return nil
}

func newSessionClock(faults FaultConfig) sessionClock {
	if faults.ControllableTime {
		return newManualSessionClock()
	}
	return realSessionClock{}
}

// faultDigest replaces a correct digest so the receiving Peer observes the
// same failure it would see if content were altered in flight.
func (a *App) faultDigest(digest string) string {
	if !a.config.Faults.CorruptDigest {
		return digest
	}
	return corruptSHA256(digest)
}

func (a *App) faultStreamReader(ctx context.Context, source io.Reader) io.Reader {
	if a.config.Faults.StreamChunkDelay <= 0 {
		return source
	}
	return &delayedReader{context: ctx, source: source, delay: a.config.Faults.StreamChunkDelay}
}

func (a *App) timeControlEnabled() bool { return a.config.Faults.ControllableTime }

func (a *App) advanceControllableTime(line string) bool {
	if !a.config.Faults.ControllableTime {
		return false
	}
	command, argument := splitCommand(line)
	if command != "advance-time" {
		return false
	}
	duration, err := time.ParseDuration(argument)
	if err != nil || duration <= 0 {
		a.line("Usage: advance-time <positive-duration>")
		return true
	}
	manual, ok := a.clock.(*manualSessionClock)
	if !ok {
		return false
	}
	manual.Advance(duration)
	return true
}

// corruptSHA256 flips the first hexadecimal digit so the value stays a
// well-formed digest that cannot match the approved content.
func corruptSHA256(digest string) string {
	if digest == "" {
		return digest
	}
	replacement := byte('0')
	if digest[0] == '0' {
		replacement = '1'
	}
	return string(replacement) + digest[1:]
}

// delayedReader slows each read so an active transfer remains observable while
// a test drives cancellation, disconnection, or idle-expiry behavior.
type delayedReader struct {
	context context.Context
	source  io.Reader
	delay   time.Duration
}

func (r *delayedReader) Read(destination []byte) (int, error) {
	timer := time.NewTimer(r.delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return r.source.Read(destination)
	case <-r.context.Done():
		return 0, r.context.Err()
	}
}
