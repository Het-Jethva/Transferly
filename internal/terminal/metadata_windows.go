//go:build windows

package terminal

import (
	"os"
	"syscall"
	"time"
)

const (
	fileAttributeReadOnly     = 0x1
	fileAttributeHidden       = 0x2
	fileAttributeReparsePoint = 0x400
)

type basicAttributes struct {
	ReadOnly     bool
	Hidden       bool
	ReparsePoint bool
}

func readBasicAttributes(path string, _ os.FileInfo) (basicAttributes, error) {
	pointer, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return basicAttributes{}, err
	}
	attributes, err := syscall.GetFileAttributes(pointer)
	if err != nil {
		return basicAttributes{}, err
	}
	return basicAttributes{
		ReadOnly:     attributes&fileAttributeReadOnly != 0,
		Hidden:       attributes&fileAttributeHidden != 0,
		ReparsePoint: attributes&fileAttributeReparsePoint != 0,
	}, nil
}

func applyBasicMetadata(path string, entry manifestEntry) error {
	if err := os.Chtimes(path, time.Now(), entry.Modified); err != nil {
		return err
	}
	pointer, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return err
	}
	attributes, err := syscall.GetFileAttributes(pointer)
	if err != nil {
		return err
	}
	attributes &^= fileAttributeReadOnly | fileAttributeHidden
	if entry.ReadOnly {
		attributes |= fileAttributeReadOnly
	}
	if entry.Hidden {
		attributes |= fileAttributeHidden
	}
	return syscall.SetFileAttributes(pointer, attributes)
}
