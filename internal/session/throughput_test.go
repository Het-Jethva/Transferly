package session

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
)

const benchmarkLargeBytes = 8 * 1024 * 1024

func BenchmarkThroughput(b *testing.B) {
	large := bytes.Repeat([]byte{0x5a}, benchmarkLargeBytes)
	small := bytes.Repeat([]byte{0xa5}, 4*1024)

	b.Run("one-large-file", func(b *testing.B) {
		benchmarkThroughputSet(b, large, 1)
	})
	b.Run("many-small-files", func(b *testing.B) {
		benchmarkThroughputSet(b, small, 256)
	})
}

func benchmarkThroughputSet(b *testing.B, content []byte, files int) {
	b.Helper()
	total := int64(len(content) * files)
	b.Run("raw-loopback-tcp", func(b *testing.B) {
		benchmarkRawTCP(b, content, files, total)
	})
	b.Run("source-read", func(b *testing.B) {
		path := benchmarkFile(b, content)
		b.SetBytes(total)
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			for file := 0; file < files; file++ {
				source, err := os.Open(path)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := io.Copy(io.Discard, source); err != nil {
					source.Close()
					b.Fatal(err)
				}
				if err := source.Close(); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
	b.Run("destination-write", func(b *testing.B) {
		path := filepath.Join(b.TempDir(), "destination.bin")
		b.SetBytes(total)
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			for file := 0; file < files; file++ {
				destination, err := os.Create(path)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := destination.Write(content); err != nil {
					destination.Close()
					b.Fatal(err)
				}
				if err := destination.Close(); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
	b.Run("transferly-tls-framed-stream", func(b *testing.B) {
		benchmarkTransferlyStream(b, content, files, total)
	})
}

func benchmarkTransferlyStream(b *testing.B, content []byte, files int, total int64) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer listener.Close()
	serverSession := make(chan *Session, 1)
	serverErrors := make(chan error, 1)
	go func() {
		raw, acceptError := listener.Accept()
		if acceptError != nil {
			serverErrors <- acceptError
			return
		}
		secured, openError := Open(context.Background(), raw, Inbound, Version{Major: 1}, func(context.Context, string) (bool, error) { return true, nil })
		if openError != nil {
			serverErrors <- openError
			return
		}
		serverSession <- secured
	}()
	raw, err := net.Dial("tcp4", listener.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	sender, err := Open(context.Background(), raw, Outbound, Version{Major: 1}, func(context.Context, string) (bool, error) { return true, nil })
	if err != nil {
		b.Fatal(err)
	}
	var receiver *Session
	select {
	case receiver = <-serverSession:
	case err := <-serverErrors:
		b.Fatal(err)
	}
	defer sender.Close()
	defer receiver.Close()

	const chunkBytes = 1024 * 1024
	chunksPerFile := (len(content) + chunkBytes - 1) / chunkBytes
	receiveErrors := make(chan error, 1)
	go func() {
		receiverDigest := sha256.New()
		for frame := 0; frame < b.N*files*chunksPerFile; frame++ {
			if frame%chunksPerFile == 0 {
				receiverDigest.Reset()
			}
			message, receiveError := receiver.Receive()
			if receiveError != nil {
				receiveErrors <- receiveError
				return
			}
			if receiveError = receiver.ReceiveChunk(context.Background(), receiverDigest, message.Size, nil); receiveError != nil {
				receiveErrors <- receiveError
				return
			}
		}
		receiveErrors <- nil
	}()

	b.SetBytes(total)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		for file := 0; file < files; file++ {
			digest := sha256.New()
			for offset := 0; offset < len(content); offset += chunkBytes {
				end := offset + chunkBytes
				if end > len(content) {
					end = len(content)
				}
				chunk := content[offset:end]
				_, _ = digest.Write(chunk)
				if err := sender.SendChunk(context.Background(), Message{Type: "content", Size: int64(len(chunk)), Offset: int64(offset)}, bytes.NewReader(chunk), int64(len(chunk))); err != nil {
					b.Fatal(err)
				}
			}
		}
	}
	b.StopTimer()
	if err := <-receiveErrors; err != nil {
		b.Fatal(err)
	}
}

func benchmarkRawTCP(b *testing.B, content []byte, files int, total int64) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer listener.Close()
	serverErrors := make(chan error, 1)
	go func() {
		connection, acceptError := listener.Accept()
		if acceptError != nil {
			serverErrors <- acceptError
			return
		}
		_, copyError := io.Copy(io.Discard, connection)
		closeError := connection.Close()
		if copyError != nil {
			serverErrors <- copyError
			return
		}
		serverErrors <- closeError
	}()
	connection, err := net.Dial("tcp4", listener.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(total)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		for file := 0; file < files; file++ {
			if _, err := connection.Write(content); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.StopTimer()
	if err := connection.Close(); err != nil {
		b.Fatal(err)
	}
	if err := <-serverErrors; err != nil {
		b.Fatal(err)
	}
}

func benchmarkFile(b *testing.B, content []byte) string {
	b.Helper()
	path := filepath.Join(b.TempDir(), "source.bin")
	if err := os.WriteFile(path, content, 0o600); err != nil {
		b.Fatal(err)
	}
	return path
}
