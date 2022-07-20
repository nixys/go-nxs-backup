package storage

import (
	"errors"
	"path"
	"path/filepath"
	"strings"

	"nxs-backup/misc"
)

var (
	ErrorObjectNotFound = errors.New("object not found")
	ErrorFileNotFound   = errors.New("file does not exist")
)

type Retention struct {
	Days   int
	Weeks  int
	Months int
}

func GetDstAndLinks(bakFile, ofs, bakPath string, days, weeks, months int) (dst string, links map[string]string, err error) {

	var rel string
	links = make(map[string]string)

	if misc.GetDateTimeNow("dom") == misc.MonthlyBackupDay && months > 0 {
		dstPath := path.Join(bakPath, ofs, "monthly")
		dst = path.Join(dstPath, bakFile)
	}
	if misc.GetDateTimeNow("dow") == misc.WeeklyBackupDay && weeks > 0 {
		dstPath := path.Join(bakPath, ofs, "weekly")
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
	if days > 0 {
		dstPath := path.Join(bakPath, ofs, "daily")
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

func GetDstList(bakFile, ofs, bakPath string, days, weeks, months int) (dst []string) {

	basePath := strings.TrimPrefix(path.Join(bakPath, ofs), "/")

	if misc.GetDateTimeNow("dom") == misc.MonthlyBackupDay && months > 0 {
		dst = append(dst, path.Join(basePath, "monthly", bakFile))
	}
	if misc.GetDateTimeNow("dow") == misc.WeeklyBackupDay && weeks > 0 {
		dst = append(dst, path.Join(basePath, "weekly", bakFile))
	}
	if days > 0 {
		dst = append(dst, path.Join(basePath, "daily", bakFile))
	}

	return
}
