package terminal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	manifestFile   = "file"
	manifestFolder = "folder"
)

type manifestEntry struct {
	SourcePath string
	Path       string
	Kind       string
	Size       int64
	Modified   time.Time
	ReadOnly   bool
	Hidden     bool
}

type manifestOmission struct {
	Path   string
	Reason string
}

type offerManifest struct {
	Roots       []string
	Entries     []manifestEntry
	Omissions   []manifestOmission
	FileCount   int
	FolderCount int
	TotalBytes  int64
}

func buildManifest(paths []string) (offerManifest, error) {
	manifest := offerManifest{}
	rootNames := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		absolute, err := filepath.Abs(path)
		if err != nil {
			return offerManifest{}, fmt.Errorf("resolve %q: %w", path, err)
		}
		info, err := os.Lstat(absolute)
		if err != nil {
			return offerManifest{}, fmt.Errorf("inspect %q: %w", path, err)
		}
		name := info.Name()
		if err := validateManifestPath(name); err != nil {
			return offerManifest{}, fmt.Errorf("top-level name %q cannot be transferred safely: %w", name, err)
		}
		key := manifestPathKey(name)
		if _, duplicate := rootNames[key]; duplicate {
			return offerManifest{}, fmt.Errorf("top-level name %q has a case-insensitive or Unicode duplicate", name)
		}
		rootNames[key] = struct{}{}
		manifest.Roots = append(manifest.Roots, name)
		if err := walkManifestRoot(&manifest, absolute, name); err != nil {
			return offerManifest{}, err
		}
	}
	if len(manifest.Entries) == 0 {
		return offerManifest{}, errors.New("no readable files or folders were found")
	}
	return manifest, nil
}

func walkManifestRoot(manifest *offerManifest, sourcePath, relativePath string) error {
	if len(manifest.Entries) >= maxManifestEntries {
		return fmt.Errorf("Transfer Offer manifest exceeds the safe limit of %d entries", maxManifestEntries)
	}
	if err := validateManifestPath(filepath.ToSlash(relativePath)); err != nil {
		return fmt.Errorf("source path %q cannot be represented safely: %w", relativePath, err)
	}
	info, err := os.Lstat(sourcePath)
	if err != nil {
		return addManifestOmission(manifest, filepath.ToSlash(relativePath), "unreadable or vanished")
	}
	attributes, err := readBasicAttributes(sourcePath, info)
	if err != nil {
		return addManifestOmission(manifest, filepath.ToSlash(relativePath), "attributes could not be read")
	}
	if info.Mode()&os.ModeSymlink != 0 || attributes.ReparsePoint {
		return addManifestOmission(manifest, filepath.ToSlash(relativePath), "symbolic link, junction, or reparse point is unsupported")
	}
	relativePath = filepath.ToSlash(relativePath)
	entry := manifestEntry{SourcePath: sourcePath, Path: relativePath, Modified: info.ModTime(), ReadOnly: attributes.ReadOnly, Hidden: attributes.Hidden}
	switch {
	case info.Mode().IsRegular():
		probe, openError := os.Open(sourcePath)
		if openError != nil {
			return addManifestOmission(manifest, relativePath, "unreadable")
		}
		_ = probe.Close()
		entry.Kind = manifestFile
		entry.Size = info.Size()
		if entry.Size > int64(^uint64(0)>>1)-manifest.TotalBytes {
			return errors.New("Transfer Offer total byte count overflows the protocol")
		}
		manifest.Entries = append(manifest.Entries, entry)
		manifest.FileCount++
		manifest.TotalBytes += info.Size()
	case info.IsDir():
		entry.Kind = manifestFolder
		manifest.Entries = append(manifest.Entries, entry)
		manifest.FolderCount++
		children, readError := os.ReadDir(sourcePath)
		if readError != nil {
			return addManifestOmission(manifest, relativePath, "folder is unreadable")
		}
		sort.Slice(children, func(i, j int) bool { return manifestPathKey(children[i].Name()) < manifestPathKey(children[j].Name()) })
		childNames := make(map[string]string, len(children))
		for _, child := range children {
			key := manifestPathKey(child.Name())
			if previous, exists := childNames[key]; exists {
				return fmt.Errorf("folder %q contains case-insensitive or Unicode aliases %q and %q", relativePath, previous, child.Name())
			}
			childNames[key] = child.Name()
			if err := walkManifestRoot(manifest, filepath.Join(sourcePath, child.Name()), filepath.Join(relativePath, child.Name())); err != nil {
				return err
			}
		}
	default:
		return addManifestOmission(manifest, relativePath, "unsupported filesystem entry")
	}
	return nil
}

func addManifestOmission(manifest *offerManifest, path, reason string) error {
	if len(manifest.Omissions) >= maxManifestOmissions {
		return fmt.Errorf("Transfer Offer manifest exceeds the safe limit of %d omissions", maxManifestOmissions)
	}
	manifest.Omissions = append(manifest.Omissions, manifestOmission{Path: path, Reason: reason})
	return nil
}

func parsePathArguments(argument string) ([]string, bool) {
	var paths []string
	for index := 0; index < len(argument); {
		for index < len(argument) && (argument[index] == ' ' || argument[index] == '\t') {
			index++
		}
		if index == len(argument) {
			break
		}
		if argument[index] == '"' {
			index++
			start := index
			for index < len(argument) && argument[index] != '"' {
				if argument[index] == '\r' || argument[index] == '\n' {
					return nil, false
				}
				index++
			}
			if index == len(argument) || index == start {
				return nil, false
			}
			paths = append(paths, argument[start:index])
			index++
			if index < len(argument) && argument[index] != ' ' && argument[index] != '\t' {
				return nil, false
			}
			continue
		}
		start := index
		for index < len(argument) && argument[index] != ' ' && argument[index] != '\t' {
			if strings.ContainsRune("\"\r\n", rune(argument[index])) {
				return nil, false
			}
			index++
		}
		paths = append(paths, argument[start:index])
	}
	return paths, len(paths) > 0
}
