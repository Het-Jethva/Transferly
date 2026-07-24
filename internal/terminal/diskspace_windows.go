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
		if callError == syscall.Errno(0) {
			callError = syscall.EINVAL
		}
		return 0, false, callError
	}
	return available, true, nil
}
