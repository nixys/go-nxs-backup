package storage

import (
	"io"
	"os"
	"path"
	"path/filepath"

	"nxs-backup/misc"
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

	subPaths := misc.GetSubPaths()

	for _, subPath := range subPaths {
		dstPath := path.Join(l.BackupPath, ofs, subPath)
		err = os.MkdirAll(dstPath, os.ModePerm)
		if err != nil {
			return
		}
	}

	dstPath := path.Join(l.BackupPath, ofs, subPath, filepath.Base(tmpBackupPath))
	destination, err := os.Create(dstPath)
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
