package session

import (
	"bufio"
	"bytes"
	"strings"
	"testing"
)

func TestProtocolFrameLengthIsBoundedOnWriteAndRead(t *testing.T) {
	oversized := Message{Type: "manifest-entry", Path: strings.Repeat("a", maximumFrameBytes)}
	if err := writeFrame(&bytes.Buffer{}, oversized); err == nil || !strings.Contains(err.Error(), "exceeds maximum size") {
		t.Fatalf("writeFrame() = %v, want precise length rejection", err)
	}

	encoded := bytes.Repeat([]byte{'x'}, maximumFrameBytes)
	encoded = append(encoded, '\n')
	var message Message
	err := readJSONFrame(bufio.NewReaderSize(bytes.NewReader(encoded), maximumFrameBytes), &message)
	if err == nil || !strings.Contains(err.Error(), "exceeds maximum size") {
		t.Fatalf("readJSONFrame() = %v, want precise length rejection", err)
	}
}

func FuzzProtocolFrameLengthHandling(f *testing.F) {
	f.Add([]byte(`{"type":"activity"}` + "\n"))
	f.Add(append(bytes.Repeat([]byte{'x'}, maximumFrameBytes), '\n'))
	f.Fuzz(func(t *testing.T, frame []byte) {
		if len(frame) > maximumFrameBytes+1 {
			frame = frame[:maximumFrameBytes+1]
		}
		var message Message
		_ = readJSONFrame(bufio.NewReaderSize(bytes.NewReader(frame), maximumFrameBytes), &message)
	})
}
