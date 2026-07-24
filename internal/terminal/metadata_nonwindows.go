//go:build !windows

package terminal

import (
	"os"
	"time"
)

type basicAttributes struct {
	ReadOnly     bool
	Hidden       bool
	ReparsePoint bool
}

func readBasicAttributes(path string, info os.FileInfo) (basicAttributes, error) {
	return basicAttributes{
		ReadOnly: info.Mode().Perm()&0o222 == 0,
		Hidden:   len(info.Name()) > 1 && info.Name()[0] == '.',
	}, nil
}

func applyBasicMetadata(path string, entry manifestEntry) error {
	mode := os.FileMode(0o644)
	if entry.Kind == manifestFolder {
		mode = 0o755
	}
	if entry.ReadOnly {
		mode &^= 0o222
	}
	if err := os.Chmod(path, mode); err != nil {
		return err
	}
	return os.Chtimes(path, time.Now(), entry.Modified)
}
