//go:build windows

package terminal

import (
	"fmt"
	"os/exec"
)

func createDestinationReparsePoint(path, target string) error {
	output, err := exec.Command("cmd", "/c", "mklink", "/J", path, target).CombinedOutput()
	if err != nil {
		return fmt.Errorf("mklink /J: %w: %s", err, output)
	}
	return nil
}
