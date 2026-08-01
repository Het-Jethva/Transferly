package terminal

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Het-Jethva/Transferly/internal/session"
)

func TestManifestPathValidationRejectsWindowsAliasesAndUnsafeShapes(t *testing.T) {
	tests := map[string]string{
		"":                                   "empty",
		"/absolute.txt":                      "absolute",
		"C:/absolute.txt":                    "absolute",
		"../escape.txt":                      "traversal",
		"folder/../escape.txt":               "traversal",
		"folder//file.txt":                   "empty component",
		"folder/./file.txt":                  "dot segment",
		"folder\\file.txt":                   "backslash",
		"folder/file.txt:stream":             "colon",
		"CON":                                "reserved",
		"folder/aux.txt":                     "reserved",
		"folder/COM1.log":                    "reserved",
		"folder/name. ":                      "trailing dot or space",
		"folder/name.":                       "trailing dot or space",
		"folder/control\x00name":             "control character",
		"folder/" + strings.Repeat("a", 256): "component is too long",
	}
	for path, reason := range tests {
		t.Run(reason+"_"+path, func(t *testing.T) {
			err := validateManifestPath(path)
			if err == nil || !strings.Contains(err.Error(), reason) {
				t.Fatalf("validateManifestPath(%q) = %v, want reason containing %q", path, err, reason)
			}
		})
	}
}

func TestManifestValidationRejectsCaseAndUnicodeAliases(t *testing.T) {
	incoming := &incomingOffer{rootCount: 2, manifest: offerManifest{
		FileCount:  2,
		TotalBytes: 2,
		Entries: []manifestEntry{
			{Path: "Résumé.txt", Kind: manifestFile, Size: 1},
			{Path: "RE\u0301SUME\u0301.TXT", Kind: manifestFile, Size: 1},
		},
	}}
	if err := validateReceivedManifest(incoming); err == nil || !strings.Contains(err.Error(), "case-insensitive or Unicode alias") {
		t.Fatalf("validateReceivedManifest() = %v, want alias rejection", err)
	}
}

func TestManifestValidationRejectsAFileUsedAsAParentFolder(t *testing.T) {
	incoming := &incomingOffer{rootCount: 1, manifest: offerManifest{
		FileCount:   2,
		FolderCount: 0,
		TotalBytes:  2,
		Entries: []manifestEntry{
			{Path: "root.txt", Kind: manifestFile, Size: 1},
			{Path: "root.txt/child.txt", Kind: manifestFile, Size: 1},
		},
	}}
	if err := validateReceivedManifest(incoming); err == nil || !strings.Contains(err.Error(), "file is used as a parent") {
		t.Fatalf("validateReceivedManifest() = %v, want invalid hierarchy rejection", err)
	}
}

func TestConflictResolutionUsesDeterministicWindowsNameKeys(t *testing.T) {
	destination := t.TempDir()
	if err := os.WriteFile(filepath.Join(destination, "RÉSUMÉ.TXT"), []byte("existing"), 0o600); err != nil {
		t.Fatal(err)
	}

	resolved, err := resolveFinalPath(destination, "re\u0301sume\u0301.txt")
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(destination, "re\u0301sume\u0301 (1).txt")
	if resolved != want {
		t.Fatalf("resolveFinalPath() = %q, want %q", resolved, want)
	}
}

func TestConflictResolutionReservesEveryPreviewedTopLevelPath(t *testing.T) {
	destination := t.TempDir()
	if err := os.WriteFile(filepath.Join(destination, "report.txt"), []byte("existing"), 0o600); err != nil {
		t.Fatal(err)
	}
	incoming := &incomingOffer{
		destination: destination,
		manifest:    offerManifest{Roots: []string{"report.txt", "report (1).txt"}},
	}
	if err := resolveIncomingPaths(incoming); err != nil {
		t.Fatal(err)
	}
	first := incoming.finalPaths[manifestPathKey("report.txt")]
	second := incoming.finalPaths[manifestPathKey("report (1).txt")]
	if manifestPathKey(first) == manifestPathKey(second) {
		t.Fatalf("two roots were previewed at the same final path %q", first)
	}
	if first != filepath.Join(destination, "report (1).txt") || second != filepath.Join(destination, "report (1) (1).txt") {
		t.Fatalf("resolved paths = %q and %q, want deterministic non-conflicting names", first, second)
	}
}

func TestPreparingIncomingContentRejectsAReparsePointInDestinationPath(t *testing.T) {
	target := t.TempDir()
	link := filepath.Join(t.TempDir(), "destination-link")
	if err := createDestinationReparsePoint(link, target); err != nil {
		t.Fatalf("create destination reparse point: %v", err)
	}
	incoming := &incomingOffer{destination: filepath.Join(link, "received"), manifest: offerManifest{}}
	if err := prepareIncoming(incoming); err == nil || !strings.Contains(err.Error(), "destination is unsafe") {
		t.Fatalf("prepareIncoming() = %v, want reparse-point rejection", err)
	}
	if _, err := os.Stat(filepath.Join(target, "received")); !os.IsNotExist(err) {
		t.Fatalf("destination escaped through a reparse point: %v", err)
	}
}

func TestPreparingAnIncomingFolderNeverMergesWithAPathCreatedAfterApproval(t *testing.T) {
	destination := t.TempDir()
	incoming := &incomingOffer{
		destination: destination,
		manifest: offerManifest{
			Roots:       []string{"photos"},
			Entries:     []manifestEntry{{Path: "photos", Kind: manifestFolder}},
			FolderCount: 1,
		},
		finalPaths: map[string]string{"photos": filepath.Join(destination, "photos")},
	}
	if err := os.Mkdir(filepath.Join(destination, "photos"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := prepareIncoming(incoming); err == nil || !strings.Contains(err.Error(), "became unavailable") {
		t.Fatalf("prepareIncoming() = %v, want conflict rejection", err)
	}
}

func TestOversizedManifestHeaderIsRejectedBeforeAllocation(t *testing.T) {
	application := &App{}
	current := &attempt{}
	err := application.handleIncomingOffer(current, session.Message{
		OfferID: strings.Repeat("a", 32), RootCount: 1, FileCount: maxManifestEntries + 1,
	})
	if err == nil || !strings.Contains(err.Error(), "oversized") {
		t.Fatalf("handleIncomingOffer() = %v, want oversized manifest rejection", err)
	}
}

func TestExecutableAndScriptExtensionsAreDetectedCaseInsensitively(t *testing.T) {
	for _, path := range []string{"setup.EXE", "folder/run.ps1", "script.JS", "macro.docm"} {
		if !isExecutableOrScript(path) {
			t.Errorf("isExecutableOrScript(%q) = false", path)
		}
	}
	if isExecutableOrScript("notes.txt") {
		t.Fatal("ordinary text file was flagged as executable")
	}
}

func FuzzTransferOfferManifestLimits(f *testing.F) {
	for _, seed := range [][4]int64{
		{1, 1, 0, 0},
		{1, maxManifestEntries, 0, 1},
		{1, maxManifestEntries + 1, 0, 1},
		{1, -1, 0, 1},
		{0, 1, 0, 1},
	} {
		f.Add(seed[0], seed[1], seed[2], seed[3])
	}
	f.Fuzz(func(t *testing.T, roots, files, folders, totalBytes int64) {
		// Header values arrive as platform ints, so clamp arbitrary fuzz values
		// before conversion and exercise the protocol's meaningful boundary.
		const fuzzLimit = int64(maxManifestEntries * 2)
		clamp := func(value int64) int64 {
			if value > fuzzLimit {
				return fuzzLimit
			}
			if value < -fuzzLimit {
				return -fuzzLimit
			}
			return value
		}
		roots, files, folders, totalBytes = clamp(roots), clamp(files), clamp(folders), clamp(totalBytes)
		entryCount := files + folders
		valid := roots >= 1 && roots <= maxManifestEntries && files >= 0 && folders >= 0 && entryCount >= 1 && entryCount <= maxManifestEntries && totalBytes >= 0

		application := &App{}
		current := &attempt{}
		err := application.handleIncomingOffer(current, session.Message{
			OfferID: strings.Repeat("a", 32), RootCount: int(roots), FileCount: int(files), FolderCount: int(folders), TotalBytes: totalBytes,
		})
		if valid && err != nil {
			t.Fatalf("valid bounded header was rejected: roots=%d files=%d folders=%d bytes=%d: %v", roots, files, folders, totalBytes, err)
		}
		if !valid && err == nil {
			t.Fatalf("invalid or oversized header was accepted: roots=%d files=%d folders=%d bytes=%d", roots, files, folders, totalBytes)
		}
	})
}

func FuzzConflictNameGeneration(f *testing.F) {
	for _, seed := range []string{"notes.txt", "Résumé.md", "archive.tar.gz", "CON", "name. "} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, name string) {
		if validateManifestPath(name) != nil || strings.Contains(name, "/") {
			return
		}
		destination := t.TempDir()
		if err := os.WriteFile(filepath.Join(destination, name), nil, 0o600); err != nil {
			return // The host filesystem can be stricter than the portable contract.
		}
		resolved, err := resolveFinalPath(destination, name)
		if err != nil {
			return // A suffix can exceed a filesystem limit even when the original fit.
		}
		if err := ensurePathBeneath(destination, resolved); err != nil {
			t.Fatalf("generated conflict name escaped destination: %v", err)
		}
		if manifestPathKey(filepath.Base(resolved)) == manifestPathKey(name) {
			t.Fatalf("generated conflict name %q aliases occupied name %q", resolved, name)
		}
	})
}

func FuzzManifestPathConfinement(f *testing.F) {
	for _, seed := range []string{"folder/file.txt", "../escape", "CON", "Résumé/file.txt", "a//b", "C:/outside"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, path string) {
		if validateManifestPath(path) != nil {
			return
		}
		destination := filepath.Join(t.TempDir(), "destination")
		candidate := filepath.Join(destination, filepath.FromSlash(path))
		relative, err := filepath.Rel(destination, candidate)
		if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
			t.Fatalf("accepted path %q escaped destination as %q (err %v)", path, candidate, err)
		}
	})
}
