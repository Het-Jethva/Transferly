package terminal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func prepareIncoming(incoming *incomingOffer) error {
	if incoming.staleStaging {
		return errors.New("stale Transferly staging data must be removed with cleanup-staging or a different destination selected")
	}
	if err := rejectExistingReparseComponents(incoming.destination); err != nil {
		return fmt.Errorf("destination is unsafe: %w", err)
	}
	if err := os.MkdirAll(incoming.destination, 0o755); err != nil {
		return fmt.Errorf("create destination: %w", err)
	}
	if err := rejectExistingReparseComponents(incoming.destination); err != nil {
		return fmt.Errorf("destination is unsafe: %w", err)
	}
	info, err := os.Stat(incoming.destination)
	if err != nil || !info.IsDir() {
		return errors.New("destination is not a folder")
	}
	if available, reliable, err := availableDiskBytes(incoming.destination); err != nil {
		return fmt.Errorf("check available disk space: %w", err)
	} else if reliable && uint64(incoming.manifest.TotalBytes) > available {
		return fmt.Errorf("insufficient disk space: need %d bytes, only %d bytes are available", incoming.manifest.TotalBytes, available)
	}

	incoming.createdDirectories = nil
	rollbackRoots := func() { removeCreatedDirectories(incoming) }
	for _, root := range incoming.manifest.Roots {
		var rootEntry *manifestEntry
		for index := range incoming.manifest.Entries {
			if manifestPathKey(incoming.manifest.Entries[index].Path) == manifestPathKey(root) {
				rootEntry = &incoming.manifest.Entries[index]
				break
			}
		}
		if rootEntry == nil {
			rollbackRoots()
			return fmt.Errorf("manifest root %q has no entry", root)
		}
		finalPath := incoming.finalPaths[manifestPathKey(root)]
		if err := ensurePathBeneath(incoming.destination, finalPath); err != nil {
			rollbackRoots()
			return err
		}
		if _, err := os.Lstat(finalPath); err == nil || !os.IsNotExist(err) {
			rollbackRoots()
			return fmt.Errorf("final path %s became unavailable after approval", finalPath)
		}
		if rootEntry.Kind == manifestFolder {
			if err := os.Mkdir(finalPath, 0o755); err != nil {
				rollbackRoots()
				return fmt.Errorf("final path %s became unavailable after approval: %w", finalPath, err)
			}
			incoming.createdDirectories = append(incoming.createdDirectories, finalPath)
		}
	}
	folders := make([]manifestEntry, 0, incoming.manifest.FolderCount)
	for _, entry := range incoming.manifest.Entries {
		if entry.Kind == manifestFolder && strings.Contains(entry.Path, "/") {
			folders = append(folders, entry)
		}
	}
	sort.Slice(folders, func(i, j int) bool {
		return strings.Count(folders[i].Path, "/") < strings.Count(folders[j].Path, "/")
	})
	for _, entry := range folders {
		path := incomingPath(incoming, entry)
		if err := ensurePathBeneath(incoming.destination, path); err != nil {
			rollbackRoots()
			return err
		}
		if err := os.Mkdir(path, 0o755); err != nil {
			rollbackRoots()
			return fmt.Errorf("create manifest folder %s: %w", entry.Path, err)
		}
		incoming.createdDirectories = append(incoming.createdDirectories, path)
	}
	staging := filepath.Join(incoming.destination, ".transferly-staging")
	if err := ensurePathBeneath(incoming.destination, staging); err != nil {
		rollbackRoots()
		return err
	}
	if err := rejectReparsePoint(staging); err != nil {
		rollbackRoots()
		return fmt.Errorf("staging area is unsafe: %w", err)
	}
	if err := os.MkdirAll(staging, 0o700); err != nil {
		rollbackRoots()
		return err
	}
	return nil
}

func incomingPath(incoming *incomingOffer, entry manifestEntry) string {
	parts := strings.Split(entry.Path, "/")
	root := incoming.finalPaths[manifestPathKey(parts[0])]
	if len(parts) == 1 {
		return root
	}
	return filepath.Join(root, filepath.Join(parts[1:]...))
}
