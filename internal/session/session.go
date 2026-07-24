// Package session establishes temporary, human-verified Transfer Sessions.
package session

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"sync"
	"time"
)

const (
	verificationLabel = "transferly verification code v1"
	maximumFrameBytes = 4096
	handshakeTimeout  = 15 * time.Second
)

// Version identifies the wire protocol independently of the executable.
type Version struct {
	Major int `json:"major"`
	Minor int `json:"minor"`
}

func (v Version) String() string {
	return fmt.Sprintf("%d.%d", v.Major, v.Minor)
}

// Role determines which side performs the TLS client handshake.
type Role uint8

const (
	Inbound Role = iota
	Outbound
)

// Confirm asks the local user whether the displayed code matches the other
// Peer. Returning false rejects the connection.
type Confirm func(context.Context, string) (bool, error)

var (
	ErrLocalRejected = errors.New("local user rejected verification")
	ErrPeerRejected  = errors.New("Peer rejected verification")
)

// VersionError reports an incompatible wire-protocol major version.
type VersionError struct {
	Local Version
	Peer  Version
}

func (e *VersionError) Error() string {
	return fmt.Sprintf("incompatible wire protocol: local %s, Peer %s", e.Local, e.Peer)
}

// Session is a verified, encrypted connection. No application protocol data
// can be exchanged through this interface until both Peers have confirmed.
type Session struct {
	connection *tls.Conn
	reader     *bufio.Reader
	writeMu    sync.Mutex
}

// Message is a bounded control frame exchanged only after a Transfer Session
// has been verified. File bytes are streamed separately by SendStream and
// ReceiveStream so their size is not limited by control-frame bounds.
type Message struct {
	Type     string `json:"type"`
	OfferID  string `json:"offer_id,omitempty"`
	Name     string `json:"name,omitempty"`
	Size     int64  `json:"size,omitempty"`
	Accepted *bool  `json:"accepted,omitempty"`
	Digest   string `json:"digest,omitempty"`
	Success  *bool  `json:"success,omitempty"`
	Reason   string `json:"reason,omitempty"`
}

// Progress observes the number of file bytes copied through a stream.
type Progress func(completed int64)

// Open protects raw with mutually authenticated TLS 1.3, negotiates the wire
// protocol, and waits for both human confirmations. Credentials exist only in
// memory and are generated anew on every call.
func Open(ctx context.Context, raw net.Conn, role Role, version Version, confirm Confirm) (*Session, error) {
	credential, err := newCredential()
	if err != nil {
		return nil, fmt.Errorf("generate temporary credential: %w", err)
	}

	configuration := &tls.Config{
		Certificates: []tls.Certificate{credential},
		MinVersion:   tls.VersionTLS13,
		MaxVersion:   tls.VersionTLS13,
	}
	var protected *tls.Conn
	if role == Outbound {
		configuration.InsecureSkipVerify = true // Human SAS verification establishes trust.
		protected = tls.Client(raw, configuration)
	} else {
		configuration.ClientAuth = tls.RequireAnyClientCert
		protected = tls.Server(raw, configuration)
	}
	opened := false
	defer func() {
		if !opened {
			_ = protected.Close()
		}
	}()

	handshakeContext, cancelHandshake := context.WithTimeout(ctx, handshakeTimeout)
	defer cancelHandshake()
	if err := protected.HandshakeContext(handshakeContext); err != nil {
		return nil, fmt.Errorf("TLS 1.3 handshake: %w", err)
	}
	state := protected.ConnectionState()
	if state.Version != tls.VersionTLS13 || len(state.PeerCertificates) == 0 {
		return nil, errors.New("Peer did not establish mutually authenticated TLS 1.3")
	}

	reader := bufio.NewReaderSize(protected, maximumFrameBytes)
	if err := writeFrame(protected, wireMessage{Type: "hello", Version: version}); err != nil {
		return nil, fmt.Errorf("send protocol version: %w", err)
	}
	peerHello, err := readWireFrame(reader)
	if err != nil {
		return nil, fmt.Errorf("read protocol version: %w", err)
	}
	if peerHello.Type != "hello" {
		return nil, fmt.Errorf("expected protocol hello, received %q", peerHello.Type)
	}
	if peerHello.Version.Major != version.Major {
		return nil, &VersionError{Local: version, Peer: peerHello.Version}
	}

	code, err := verificationCode(state)
	if err != nil {
		return nil, fmt.Errorf("derive verification code: %w", err)
	}
	if err := exchangeConfirmation(ctx, protected, reader, code, confirm); err != nil {
		return nil, err
	}

	opened = true
	return &Session{connection: protected, reader: reader}, nil
}

// Send writes one bounded control message to the verified Peer.
func (s *Session) Send(message Message) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return writeFrame(s.connection, message)
}

// Receive reads one bounded control message from the verified Peer.
func (s *Session) Receive() (Message, error) {
	var message Message
	err := readJSONFrame(s.reader, &message)
	return message, err
}

// SendStream writes a content header followed by exactly size bytes while
// computing SHA-256 with a fixed-size buffer.
func (s *Session) SendStream(ctx context.Context, header Message, source io.Reader, size int64, progress Progress) (string, error) {
	if size < 0 {
		return "", errors.New("stream size cannot be negative")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := writeFrame(s.connection, header); err != nil {
		return "", err
	}
	hash := sha256.New()
	if err := copyStream(ctx, io.MultiWriter(s.connection, hash), source, size, progress); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// ReceiveStream reads exactly size bytes from the current content frame into
// destination while independently computing SHA-256 with bounded memory.
func (s *Session) ReceiveStream(ctx context.Context, destination io.Writer, size int64, progress Progress) (string, error) {
	if size < 0 {
		return "", errors.New("stream size cannot be negative")
	}
	hash := sha256.New()
	if err := copyStream(ctx, io.MultiWriter(destination, hash), s.reader, size, progress); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func copyStream(ctx context.Context, destination io.Writer, source io.Reader, size int64, progress Progress) error {
	const bufferBytes = 64 * 1024
	buffer := make([]byte, bufferBytes)
	remaining := size
	completed := int64(0)
	if progress != nil {
		progress(0)
	}
	for remaining > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		chunk := int64(len(buffer))
		if remaining < chunk {
			chunk = remaining
		}
		read, readError := io.ReadFull(source, buffer[:chunk])
		if read > 0 {
			written, writeError := destination.Write(buffer[:read])
			if writeError != nil {
				return writeError
			}
			if written != read {
				return io.ErrShortWrite
			}
			completed += int64(read)
			remaining -= int64(read)
			if progress != nil {
				progress(completed)
			}
		}
		if readError != nil {
			return readError
		}
	}
	return nil
}

// Close ends the temporary trust relationship and removes all in-memory TLS
// state with the Session.
func (s *Session) Close() error {
	return s.connection.Close()
}

type confirmationResult struct {
	accepted bool
	err      error
}

func exchangeConfirmation(ctx context.Context, connection net.Conn, reader *bufio.Reader, code string, confirm Confirm) error {
	confirmationContext, cancel := context.WithCancel(ctx)
	defer cancel()

	localResult := make(chan confirmationResult, 1)
	go func() {
		accepted, err := confirm(confirmationContext, code)
		localResult <- confirmationResult{accepted: accepted, err: err}
	}()

	peerResult := make(chan confirmationResult, 1)
	go func() {
		message, err := readWireFrame(reader)
		if err != nil {
			peerResult <- confirmationResult{err: fmt.Errorf("read Peer confirmation: %w", err)}
			return
		}
		if message.Type != "confirmation" || message.Accepted == nil {
			peerResult <- confirmationResult{err: fmt.Errorf("expected Peer confirmation, received %q", message.Type)}
			return
		}
		peerResult <- confirmationResult{accepted: *message.Accepted}
	}()

	localAccepted := false
	peerAccepted := false
	for !localAccepted || !peerAccepted {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case result := <-localResult:
			if result.err != nil {
				return result.err
			}
			if err := writeFrame(connection, confirmationMessage(result.accepted)); err != nil {
				return fmt.Errorf("send confirmation: %w", err)
			}
			if !result.accepted {
				return ErrLocalRejected
			}
			localAccepted = true
			localResult = nil
		case result := <-peerResult:
			if result.err != nil {
				return result.err
			}
			if !result.accepted {
				return ErrPeerRejected
			}
			peerAccepted = true
			peerResult = nil
		}
	}
	return nil
}

type wireMessage struct {
	Type     string  `json:"type"`
	Version  Version `json:"version,omitempty"`
	Accepted *bool   `json:"accepted,omitempty"`
}

func confirmationMessage(accepted bool) wireMessage {
	return wireMessage{Type: "confirmation", Accepted: &accepted}
}

func writeFrame(writer io.Writer, message any) error {
	encoded, err := json.Marshal(message)
	if err != nil {
		return err
	}
	if len(encoded)+1 > maximumFrameBytes {
		return errors.New("protocol frame exceeds maximum size")
	}
	encoded = append(encoded, '\n')
	for len(encoded) > 0 {
		written, writeError := writer.Write(encoded)
		if writeError != nil {
			return writeError
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		encoded = encoded[written:]
	}
	return nil
}

func readWireFrame(reader *bufio.Reader) (wireMessage, error) {
	var message wireMessage
	err := readJSONFrame(reader, &message)
	return message, err
}

func readJSONFrame(reader *bufio.Reader, destination any) error {
	line, err := reader.ReadSlice('\n')
	if errors.Is(err, bufio.ErrBufferFull) || len(line) > maximumFrameBytes {
		return errors.New("protocol frame exceeds maximum size")
	}
	if err != nil {
		return err
	}
	if err := json.Unmarshal(line, destination); err != nil {
		return fmt.Errorf("invalid protocol frame: %w", err)
	}
	return nil
}

func verificationCode(state tls.ConnectionState) (string, error) {
	material, err := state.ExportKeyingMaterial(verificationLabel, nil, 32)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(material)
	value := binary.BigEndian.Uint32(digest[:4]) % 1_000_000
	return fmt.Sprintf("%06d", value), nil
}

func newCredential() (tls.Certificate, error) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return tls.Certificate{}, err
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject:      pkix.Name{CommonName: "Transferly temporary Peer"},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
	}
	certificateDER, err := x509.CreateCertificate(rand.Reader, template, template, publicKey, privateKey)
	if err != nil {
		return tls.Certificate{}, err
	}
	certificate, err := x509.ParseCertificate(certificateDER)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.Certificate{
		Certificate: [][]byte{certificateDER},
		PrivateKey:  privateKey,
		Leaf:        certificate,
	}, nil
}
