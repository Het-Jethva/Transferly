//go:build windows

package integration_test

import (
	"os/exec"
	"syscall"
)

const (
	testFileAttributeReadOnly = 0x1
	testFileAttributeHidden   = 0x2
)

func addFileAttributes(path string, attributes uint32) error {
	pointer, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return err
	}
	current, err := syscall.GetFileAttributes(pointer)
	if err != nil {
		return err
	}
	return syscall.SetFileAttributes(pointer, current|attributes)
}

func createReparsePoint(path, target string) error {
	return exec.Command("cmd", "/c", "mklink", "/J", path, target).Run()
}

func makeUnreadable(path string) (func(), bool) {
	pointer, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return func() {}, false
	}
	handle, err := syscall.CreateFile(pointer, syscall.GENERIC_READ, 0, nil, syscall.OPEN_EXISTING, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return func() {}, false
	}
	return func() { _ = syscall.CloseHandle(handle) }, true
}

func fileHasAttributes(path string, attributes uint32) (bool, error) {
	pointer, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return false, err
	}
	current, err := syscall.GetFileAttributes(pointer)
	return current&attributes == attributes, err
}
