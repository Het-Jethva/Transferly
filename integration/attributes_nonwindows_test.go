//go:build !windows

package integration_test

import "os"

const (
	testFileAttributeReadOnly = 1 << iota
	testFileAttributeHidden
)

func addFileAttributes(path string, attributes uint32) error {
	if attributes&testFileAttributeReadOnly != 0 {
		return os.Chmod(path, 0o400)
	}
	return nil
}

func createReparsePoint(path, target string) error {
	return os.Symlink(target, path)
}

func makeUnreadable(path string) (func(), bool) {
	if err := os.Chmod(path, 0); err != nil {
		return func() {}, false
	}
	restore := func() { _ = os.Chmod(path, 0o600) }
	file, openErr := os.Open(path)
	if openErr == nil {
		_ = file.Close()
		restore()
		return func() {}, false
	}
	return restore, true
}

func fileHasAttributes(path string, attributes uint32) (bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		return false, err
	}
	if attributes&testFileAttributeReadOnly != 0 && info.Mode().Perm()&0o222 != 0 {
		return false, nil
	}
	return true, nil
}
