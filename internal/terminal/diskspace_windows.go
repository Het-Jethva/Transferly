//go:build windows

package terminal

import (
	"syscall"
	"unsafe"
)

var getDiskFreeSpaceEx = syscall.NewLazyDLL("kernel32.dll").NewProc("GetDiskFreeSpaceExW")

func availableDiskBytes(path string) (uint64, bool, error) {
	pointer, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return 0, false, err
	}
	var available uint64
	result, _, callError := getDiskFreeSpaceEx.Call(
		uintptr(unsafe.Pointer(pointer)),
		uintptr(unsafe.Pointer(&available)),
		0,
		0,
	)
	if result == 0 {
		// Call always yields a raw syscall.Errno, never a wrapped error, so a
		// direct comparison is the correct zero check here.
		if callError == syscall.Errno(0) { //nolint:errorlint // raw syscall return
			callError = syscall.EINVAL
		}
		return 0, false, callError
	}
	return available, true, nil
}
