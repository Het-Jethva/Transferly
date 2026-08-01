package session

import (
	"bufio"
	"bytes"
	"fmt"
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

func TestProtocolFrameRejectsMalformedAndTruncatedStreams(t *testing.T) {
	for name, frame := range map[string][]byte{
		"malformed JSON":    []byte("{not-json}\n"),
		"truncated JSON":    []byte(`{"type":"activity"`),
		"missing delimiter": []byte(`{"type":"activity"}`),
	} {
		t.Run(name, func(t *testing.T) {
			var message Message
			if err := readJSONFrame(bufio.NewReaderSize(bytes.NewReader(frame), maximumFrameBytes), &message); err == nil {
				t.Fatalf("readJSONFrame(%q) unexpectedly succeeded", frame)
			}
		})
	}
}

func TestVerificationCodeDerivationHasAKnownSixDigitResult(t *testing.T) {
	code := verificationCodeFromMaterial([]byte("transferly-test-exporter-material"))
	if code != "251662" {
		t.Fatalf("verificationCodeFromMaterial() = %q, want known result 251662", code)
	}
}

func FuzzProtocolFrameLengthHandling(f *testing.F) {
	f.Add([]byte(`{"type":"activity"}`))
	f.Add([]byte("{}\n"))
	f.Add(bytes.Repeat([]byte{'x'}, maximumFrameBytes))
	f.Fuzz(func(t *testing.T, payload []byte) {
		if len(payload) > maximumFrameBytes+1 {
			payload = payload[:maximumFrameBytes+1]
		}
		// The property requires the canary to be the next frame.
		if delimiter := bytes.IndexByte(payload, '\n'); delimiter >= 0 {
			payload = payload[:delimiter]
		}
		stream := append(append(append([]byte(nil), payload...), '\n'), []byte(`{"type":"canary"}`+"\n")...)
		reader := bufio.NewReaderSize(bytes.NewReader(stream), maximumFrameBytes)
		var message Message
		if err := readJSONFrame(reader, &message); err == nil {
			var canary Message
			if err := readJSONFrame(reader, &canary); err != nil || canary.Type != "canary" {
				t.Fatalf("successful bounded frame consumed the following frame: message=%+v canary=%+v err=%v", message, canary, err)
			}
		}
	})
}

func FuzzVerificationCodeDerivation(f *testing.F) {
	f.Add([]byte("transferly-test-exporter-material"))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, material []byte) {
		code := verificationCodeFromMaterial(material)
		if len(code) != 6 {
			t.Fatalf("verification code %q is not six digits", code)
		}
		var value int
		if _, err := fmt.Sscanf(code, "%06d", &value); err != nil || value < 0 || value > 999999 {
			t.Fatalf("verification code %q is outside the six-digit decimal range: %v", code, err)
		}
		if second := verificationCodeFromMaterial(append([]byte(nil), material...)); second != code {
			t.Fatalf("verification code is not deterministic: %q then %q", code, second)
		}
	})
}
