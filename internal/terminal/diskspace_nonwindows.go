//go:build !windows

package terminal

func availableDiskBytes(string) (uint64, bool, error) {
	return 0, false, nil
}
