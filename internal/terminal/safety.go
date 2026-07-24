package terminal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf16"
	"unicode/utf8"

	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

const (
	maxManifestEntries   = 100_000
	maxManifestOmissions = 100_000
	maxManifestPathBytes = 3_000 // Leaves room for the rest of a bounded control frame.
	maxWindowsComponent  = 255
	maxWindowsPath       = 32_767
	maxOmissionReason    = 1_024
)

var executableAndScriptExtensions = map[string]struct{}{
	".bat": {}, ".cmd": {}, ".com": {}, ".cpl": {}, ".exe": {}, ".hta": {}, ".inf": {},
	".ins": {}, ".iso": {}, ".jar": {}, ".js": {}, ".jse": {}, ".lnk": {}, ".msc": {},
	".msi": {}, ".msp": {}, ".mst": {}, ".ps1": {}, ".ps1xml": {}, ".ps2": {}, ".ps2xml": {},
	".psc1": {}, ".psc2": {}, ".reg": {}, ".scf": {}, ".scr": {}, ".sct": {}, ".sh": {},
	".url": {}, ".vb": {}, ".vbe": {}, ".vbs": {}, ".ws": {}, ".wsc": {}, ".wsf": {}, ".wsh": {},
	".docm": {}, ".dotm": {}, ".potm": {}, ".ppam": {}, ".ppsm": {}, ".pptm": {}, ".xlam": {}, ".xlsm": {},
}

func validateManifestPath(path string) error {
	if path == "" {
		return errors.New("path is empty")
	}
	if !utf8.ValidString(path) {
		return errors.New("path is not valid UTF-8")
	}
	if len(path) > maxManifestPathBytes || utf16Length(path) > maxWindowsPath {
		return errors.New("path is too long for the protocol or Windows")
	}
	if strings.HasPrefix(path, "/") || (len(path) >= 2 && isASCIILetter(path[0]) && path[1] == ':') {
		return errors.New("absolute path is not allowed")
	}
	if strings.Contains(path, "\\") {
		return errors.New("path contains a backslash; wire paths must use forward slashes")
	}

	components := strings.Split(path, "/")
	for _, component := range components {
		if component == "" {
			return errors.New("path contains an empty component")
		}
		if component == "." {
			return errors.New("path contains a dot segment")
		}
		if component == ".." {
			return errors.New("path contains a traversal segment")
		}
		if err := validateWindowsComponent(component); err != nil {
			return err
		}
	}
	return nil
}

func validateWindowsComponent(component string) error {
	if utf16Length(component) > maxWindowsComponent {
		return errors.New("path component is too long for Windows")
	}
	if strings.HasSuffix(component, ".") || strings.HasSuffix(component, " ") {
		return errors.New("path component has a trailing dot or space Windows alias")
	}
	for _, character := range component {
		if character < 32 || character == 127 {
			return errors.New("path contains a control character")
		}
		if strings.ContainsRune(`<>:"/\\|?*`, character) {
			if character == ':' {
				return errors.New("path component contains a colon")
			}
			return fmt.Errorf("path component contains invalid Windows character %q", character)
		}
	}
	base := component
	if dot := strings.IndexByte(base, '.'); dot >= 0 {
		base = base[:dot]
	}
	base = strings.ToUpper(base)
	if isWindowsReservedBase(base) {
		return fmt.Errorf("path component %q uses a Windows reserved name", component)
	}
	return nil
}

func isWindowsReservedBase(base string) bool {
	switch base {
	case "CON", "PRN", "AUX", "NUL", "CLOCK$":
		return true
	}
	if len(base) == 4 && (strings.HasPrefix(base, "COM") || strings.HasPrefix(base, "LPT")) && base[3] >= '1' && base[3] <= '9' {
		return true
	}
	// Windows also recognizes these superscript digits as device aliases.
	switch base {
	case "COM¹", "COM²", "COM³", "LPT¹", "LPT²", "LPT³":
		return true
	default:
		return false
	}
}

func manifestPathKey(path string) string {
	return cases.Fold().String(norm.NFC.String(path))
}

func utf16Length(value string) int {
	return len(utf16.Encode([]rune(value)))
}

func isASCIILetter(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z'
}

func isExecutableOrScript(path string) bool {
	_, found := executableAndScriptExtensions[strings.ToLower(filepath.Ext(path))]
	return found
}

func executablePaths(manifest offerManifest) []string {
	paths := make([]string, 0)
	for _, entry := range manifest.Entries {
		if entry.Kind == manifestFile && isExecutableOrScript(entry.Path) {
			paths = append(paths, entry.Path)
		}
	}
	return paths
}

func ensurePathBeneath(destination, candidate string) error {
	destination, err := filepath.Abs(destination)
	if err != nil {
		return err
	}
	candidate, err = filepath.Abs(candidate)
	if err != nil {
		return err
	}
	relative, err := filepath.Rel(destination, candidate)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
		return errors.New("resolved path escapes the selected destination")
	}
	if utf16Length(candidate) > maxWindowsPath {
		return errors.New("resolved path is too long for Windows")
	}
	return nil
}

func rejectExistingReparseComponents(path string) error {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	volume := filepath.VolumeName(absolute)
	current := string(filepath.Separator)
	remaining := strings.TrimPrefix(absolute, current)
	if volume != "" {
		current = volume + string(filepath.Separator)
		remaining = strings.TrimPrefix(absolute, current)
	}
	for _, component := range strings.Split(remaining, string(filepath.Separator)) {
		if component == "" || component == "." {
			continue
		}
		current = filepath.Join(current, component)
		if _, err := os.Lstat(current); os.IsNotExist(err) {
			return nil
		} else if err != nil {
			return err
		}
		if err := rejectReparsePoint(current); err != nil {
			return err
		}
	}
	return nil
}

func rejectReparsePoint(path string) error {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	attributes, err := readBasicAttributes(path, info)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || attributes.ReparsePoint {
		return fmt.Errorf("%s is a symbolic link, junction, or reparse point", path)
	}
	return nil
}

func rejectReparseAncestors(destination, candidate string) error {
	if err := ensurePathBeneath(destination, candidate); err != nil {
		return err
	}
	relative, err := filepath.Rel(destination, candidate)
	if err != nil {
		return err
	}
	current := destination
	if err := rejectReparsePoint(current); err != nil {
		return err
	}
	for _, component := range strings.Split(relative, string(filepath.Separator)) {
		if component == "." || component == "" {
			continue
		}
		current = filepath.Join(current, component)
		if err := rejectReparsePoint(current); err != nil {
			return err
		}
	}
	return nil
}
