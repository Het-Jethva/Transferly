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
		key := strings.ToLower(name)
		if _, duplicate := rootNames[key]; duplicate {
			return offerManifest{}, fmt.Errorf("top-level name %q is duplicated", name)
		}
		rootNames[key] = struct{}{}
		manifest.Roots = append(manifest.Roots, name)
		walkManifestRoot(&manifest, absolute, name)
	}
	if len(manifest.Entries) == 0 {
		return offerManifest{}, errors.New("no readable files or folders were found")
	}
	return manifest, nil
}

func walkManifestRoot(manifest *offerManifest, sourcePath, relativePath string) {
	info, err := os.Lstat(sourcePath)
	if err != nil {
		manifest.Omissions = append(manifest.Omissions, manifestOmission{Path: filepath.ToSlash(relativePath), Reason: "unreadable or vanished"})
		return
	}
	attributes, err := readBasicAttributes(sourcePath, info)
	if err != nil {
		manifest.Omissions = append(manifest.Omissions, manifestOmission{Path: filepath.ToSlash(relativePath), Reason: "attributes could not be read"})
		return
	}
	if info.Mode()&os.ModeSymlink != 0 || attributes.ReparsePoint {
		manifest.Omissions = append(manifest.Omissions, manifestOmission{Path: filepath.ToSlash(relativePath), Reason: "symbolic link, junction, or reparse point is unsupported"})
		return
	}
	relativePath = filepath.ToSlash(relativePath)
	entry := manifestEntry{SourcePath: sourcePath, Path: relativePath, Modified: info.ModTime(), ReadOnly: attributes.ReadOnly, Hidden: attributes.Hidden}
	switch {
	case info.Mode().IsRegular():
		probe, openError := os.Open(sourcePath)
		if openError != nil {
			manifest.Omissions = append(manifest.Omissions, manifestOmission{Path: relativePath, Reason: "unreadable"})
			return
		}
		_ = probe.Close()
		entry.Kind = manifestFile
		entry.Size = info.Size()
		manifest.Entries = append(manifest.Entries, entry)
		manifest.FileCount++
		manifest.TotalBytes += info.Size()
	case info.IsDir():
		entry.Kind = manifestFolder
		manifest.Entries = append(manifest.Entries, entry)
		manifest.FolderCount++
		children, readError := os.ReadDir(sourcePath)
		if readError != nil {
			manifest.Omissions = append(manifest.Omissions, manifestOmission{Path: relativePath, Reason: "folder is unreadable"})
			return
		}
		sort.Slice(children, func(i, j int) bool { return strings.ToLower(children[i].Name()) < strings.ToLower(children[j].Name()) })
		for _, child := range children {
			walkManifestRoot(manifest, filepath.Join(sourcePath, child.Name()), filepath.Join(relativePath, child.Name()))
		}
	default:
		manifest.Omissions = append(manifest.Omissions, manifestOmission{Path: relativePath, Reason: "unsupported filesystem entry"})
	}
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
