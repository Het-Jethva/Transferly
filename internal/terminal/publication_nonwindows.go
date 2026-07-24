//go:build !windows

package terminal

import "os"

// publishWithoutOverwrite atomically creates finalPath as a hard link to the
// destination-local staged file. Linking fails rather than replacing an
// existing path.
func publishWithoutOverwrite(stagingPath, finalPath string) error {
	if err := os.Link(stagingPath, finalPath); err != nil {
		return err
	}
	if err := os.Remove(stagingPath); err != nil {
		_ = os.Remove(finalPath)
		return err
	}
	return nil
}
