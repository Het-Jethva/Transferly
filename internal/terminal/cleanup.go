package terminal

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func (a *App) cleanupIncoming(current *attempt) {
	a.mu.Lock()
	incoming := current.incoming
	current.incoming = nil
	var reviewDone <-chan struct{}
	if incoming != nil {
		reviewDone = incoming.reviewDone
	}
	a.mu.Unlock()
	if reviewDone != nil {
		<-reviewDone
	}
	if incoming != nil {
		a.removeIncomingFiles(incoming)
	}
}

func (a *App) removeIncomingFiles(incoming *incomingOffer) {
	a.removeIncomingStaging(incoming)
	removeCreatedDirectories(incoming)
}

func (a *App) removeIncomingStaging(incoming *incomingOffer) {
	for key, stream := range incoming.receivingFiles {
		if stream.file != nil {
			_ = stream.file.Close()
			stream.file = nil
		}
		if stream.stagingPath != "" {
			_ = os.Remove(stream.stagingPath)
			stream.stagingPath = ""
		}
		delete(incoming.receivingFiles, key)
	}
	removeEmptyStagingDirectory(incoming.destination)
}

func (a *App) removeIncomingStream(incoming *incomingOffer, path string) {
	key := manifestPathKey(path)
	stream := incoming.receivingFiles[key]
	if stream == nil {
		return
	}
	if stream.file != nil {
		_ = stream.file.Close()
	}
	if stream.stagingPath != "" {
		_ = os.Remove(stream.stagingPath)
	}
	delete(incoming.receivingFiles, key)
	removeEmptyStagingDirectory(incoming.destination)
}

func removeCreatedDirectories(incoming *incomingOffer) {
	for index := len(incoming.createdDirectories) - 1; index >= 0; index-- {
		_ = os.Remove(incoming.createdDirectories[index]) // Removes only empty paths; completed files are never touched.
	}
	incoming.createdDirectories = nil
}

func removeEmptyStagingDirectory(destination string) {
	_ = os.Remove(filepath.Join(destination, ".transferly-staging"))
}

func hasStaleStaging(destination string) (bool, error) {
	staging := filepath.Join(destination, ".transferly-staging")
	info, err := os.Lstat(staging)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return false, errors.New("the Transferly staging path is not a safe folder")
	}
	if err := rejectReparsePoint(staging); err != nil {
		return false, err
	}
	entries, err := os.ReadDir(staging)
	if err != nil {
		return false, err
	}
	if len(entries) == 0 {
		_ = os.Remove(staging)
		return false, nil
	}
	return true, nil
}

func cleanupStaleStaging(destination string) error {
	staging := filepath.Join(destination, ".transferly-staging")
	if err := ensurePathBeneath(destination, staging); err != nil {
		return err
	}
	if err := rejectReparsePoint(staging); err != nil {
		return err
	}
	return os.RemoveAll(staging)
}

func defaultDestination() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, "Downloads"), nil
}

func resolveFinalPath(destination, name string) (string, error) {
	destination, reserved, err := destinationNameReservations(destination)
	if err != nil {
		return "", err
	}
	return resolveFinalPathWithReservations(destination, name, reserved)
}

func destinationNameReservations(destination string) (string, map[string]struct{}, error) {
	destination, err := filepath.Abs(destination)
	if err != nil {
		return "", nil, err
	}
	entries, err := os.ReadDir(destination)
	if err != nil && !os.IsNotExist(err) {
		return "", nil, err
	}
	reserved := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		reserved[manifestPathKey(entry.Name())] = struct{}{}
	}
	return destination, reserved, nil
}

func resolveFinalPathWithReservations(destination, name string, reserved map[string]struct{}) (string, error) {
	if err := validateManifestPath(name); err != nil || strings.Contains(name, "/") {
		if err == nil {
			err = errors.New("top-level name contains a path separator")
		}
		return "", err
	}
	candidate := name
	extension := filepath.Ext(name)
	stem := strings.TrimSuffix(name, extension)
	for suffix := 1; ; suffix++ {
		if err := validateWindowsComponent(candidate); err != nil {
			return "", err
		}
		resolved := filepath.Join(destination, candidate)
		if err := ensurePathBeneath(destination, resolved); err != nil {
			return "", err
		}
		key := manifestPathKey(candidate)
		if _, found := reserved[key]; !found {
			reserved[key] = struct{}{}
			return resolved, nil
		}
		candidate = fmt.Sprintf("%s (%d)%s", stem, suffix, extension)
	}
}

func newOfferID() (string, error) {
	identifier := make([]byte, 16)
	if _, err := rand.Read(identifier); err != nil {
		return "", err
	}
	return hex.EncodeToString(identifier), nil
}
