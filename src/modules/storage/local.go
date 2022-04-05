package storage

import (
	"io"
	"os"
)

type Retention struct {
	Days   int
	Weeks  int
	Months int
}

type Local struct {
	BackupPath string
	Retention
}

func (l Local) IsLocal() int { return 1 }

func (l Local) CopyFile(tmpBackupPath, ofs string, move bool) (err error) {

	source, err := os.Open(tmpBackupPath)
	if err != nil {
		return
	}
	defer source.Close()

	destination, err := os.Create(l.BackupPath)
	if err != nil {
		return
	}
	defer destination.Close()

	_, err = io.Copy(destination, source)
	if err != nil {
		return
	}

	if move {
		err = os.Remove(tmpBackupPath)
	}

	return
}

func (l Local) ListFiles() (err error) {
	return
}

func (l Local) DeleteFile() (err error) {
	return
}
