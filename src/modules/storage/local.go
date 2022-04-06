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

func (l Local) CopyFile(tmpBackup, ofs string, move bool) (err error) {

	dstPath, links, err := l.GetDstAndLinks(filepath.Base(tmpBackup), ofs)
	if err != nil {
		return
	}

	source, err := os.Open(tmpBackup)
	if err != nil {
		return
	}
	defer source.Close()

	destination, err := os.Create(dstPath)
	if err != nil {
		return
	}
	defer destination.Close()

	_, err = io.Copy(destination, source)
	if err != nil {
		return
	}

	for dst, src := range links {
		err = os.Symlink(src, dst)
		if err != nil {
			return err
		}
	}

	if move {
		err = os.Remove(tmpBackup)
	}

	return
}

func (l Local) ListFiles() (err error) {
	return
}

func (l Local) DeleteFile() (err error) {
	return
}

func (l Local) GetDstAndLinks(bakFile, ofs string) (dst string, links map[string]string, err error) {

	links = make(map[string]string)

	if misc.GetDateTimeNow("dom") == misc.MonthlyBackupDay && l.Months > 0 {
		dstPath := path.Join(l.BackupPath, ofs, "monthly")
		err = os.MkdirAll(dstPath, os.ModePerm)
		if err != nil {
			return
		}

		dst = path.Join(dstPath, bakFile)
	}
	if misc.GetDateTimeNow("dow") == misc.WeeklyBackupDay && l.Weeks > 0 {
		dstPath := path.Join(l.BackupPath, ofs, "weekly")
		err = os.MkdirAll(dstPath, os.ModePerm)
		if err != nil {
			return
		}

		if dst != "" {
			links[path.Join(dstPath, bakFile)] = dst
		} else {
			dst = path.Join(dstPath, bakFile)
		}
	}
	if l.Days > 0 {
		dstPath := path.Join(l.BackupPath, ofs, "daily")
		err = os.MkdirAll(dstPath, os.ModePerm)
		if err != nil {
			return
		}

		if dst != "" {
			links[path.Join(dstPath, bakFile)] = dst
		} else {
			dst = path.Join(dstPath, bakFile)
		}
	}

	return
}
