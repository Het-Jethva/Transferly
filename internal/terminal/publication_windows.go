//go:build windows

package terminal

import (
	"os"
	"syscall"
	"unsafe"
)

const moveFileWriteThrough = 0x8

var moveFileEx = syscall.NewLazyDLL("kernel32.dll").NewProc("MoveFileExW")

// publishWithoutOverwrite uses an atomic same-volume move without the
// MOVEFILE_REPLACE_EXISTING flag, so a path created after approval is never
// overwritten. Unlike hard linking, this works on Windows filesystems that do
// not support hard links.
func publishWithoutOverwrite(stagingPath, finalPath string) error {
	from, err := syscall.UTF16PtrFromString(stagingPath)
	if err != nil {
		return err
	}
	to, err := syscall.UTF16PtrFromString(finalPath)
	if err != nil {
		return err
	}
	result, _, callError := moveFileEx.Call(
		uintptr(unsafe.Pointer(from)),
		uintptr(unsafe.Pointer(to)),
		uintptr(moveFileWriteThrough),
	)
	if result == 0 {
		// Call always yields a raw syscall.Errno, never a wrapped error, so a
		// direct comparison is the correct zero check here.
		if callError == syscall.Errno(0) { //nolint:errorlint // raw syscall return
			callError = syscall.EINVAL
		}
		return &os.LinkError{Op: "publish", Old: stagingPath, New: finalPath, Err: callError}
	}
	return nil
}
