package storage

import (
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"time"

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

func (l Local) GetDstAndLinks(bakFile, ofs string) (dst string, links map[string]string, err error) {

	var rel string
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
			rel, err = filepath.Rel(dstPath, dst)
			if err != nil {
				return
			}
			links[path.Join(dstPath, bakFile)] = rel
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
			rel, err = filepath.Rel(dstPath, dst)
			if err != nil {
				return
			}
			links[path.Join(dstPath, bakFile)] = rel
		} else {
			dst = path.Join(dstPath, bakFile)
		}
	}

	return
}

func (l Local) ListFiles() (err error) {
	return
}

func (l Local) ControlFiles(ofsPartsList []string) (err error) {

	for _, ofsPart := range ofsPartsList {
		dailyPath := filepath.Join(l.BackupPath, ofsPart, "daily")
		dailyRetention := time.Hour * 24 * time.Duration(l.Retention.Days)
		err = removeOldFiles(dailyPath, dailyRetention)

		weeklyPath := filepath.Join(l.BackupPath, ofsPart, "weekly")
		weeklyRetention := time.Hour * 24 * 7 * time.Duration(l.Retention.Weeks)
		err = removeOldFiles(weeklyPath, weeklyRetention)

		monthlyPath := filepath.Join(l.BackupPath, ofsPart, "monthly")
		monthlyRetention := time.Hour * 24 * 7 * time.Duration(l.Retention.Months)
		fmt.Println(monthlyPath, monthlyRetention)
		//err = removeOldFiles(monthlyPath, monthlyRetention)
	}
	return
}

func removeOldFiles(path string, retention time.Duration) (err error) {
	var files []fs.FileInfo

	fmt.Println(path, retention)
	files, err = ioutil.ReadDir(path)
	if err != nil {
		return
	}
	for _, file := range files {
		curDate := time.Now().Round(24 * time.Hour)
		fileDuration := curDate.Sub(file.ModTime())
		fmt.Println(file.Name(), " : ", fileDuration, " : ", fileDuration > retention)
		if fileDuration > retention {
			err = os.Remove(filepath.Join(path, file.Name()))
			if err != nil {
				return
			}
		}
	}
	return
}
