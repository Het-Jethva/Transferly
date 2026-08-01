//go:build !windows

package terminal

import "os"

func createDestinationReparsePoint(path, target string) error {
	return os.Symlink(target, path)
}
