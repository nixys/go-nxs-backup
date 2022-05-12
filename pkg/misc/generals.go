package misc

import (
	"fmt"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	appctx "github.com/nixys/nxs-go-appctx/v2"

	"nxs-backup/interfaces"
)

const (
	MonthlyBackupDay = "1"
	WeeklyBackupDay  = "7"
)

var AllowedJobTypes = []string{
	"desc_files",
	"inc_files",
	"mysql",
	"mysql_xtrabackup",
	"postgresql",
	"postgresql_basebackup",
	"mongodb",
	"redis",
	"external",
}

var AllowedStorageTypes = []string{
	"s3",
	"scp",
	"sftp",
	"ftp",
	"smb",
	"nfs",
	"webdav",
	"local",
}

func GetOfsPart(regex, target string) string {
	var pathParts []string

	regexParts := strings.Split(regex, "/")
	targetParts := strings.Split(target, "/")

	for i, p := range regexParts {
		if p != targetParts[i] {
			pathParts = append(pathParts, targetParts[i])
		}
	}

	if len(pathParts) > 0 {
		return strings.Join(pathParts, "___")
	}

	return targetParts[len(targetParts)-1]
}

func GetDateTimeNow(unit string) (res string) {

	currentTime := time.Now()

	switch unit {
	case "dom":
		res = strconv.Itoa(currentTime.Day())
	case "dow":
		res = strconv.Itoa(int(currentTime.Weekday()))
	case "moy":
		res = strconv.Itoa(int(currentTime.Month()))
	case "year":
		res = strconv.Itoa(currentTime.Year())
	case "log":
		res = currentTime.Format("2006-01-2 15:04:05.000000")
	default:
		res = currentTime.Format("2006-01-2_15-04")
	}

	return res
}

func NeedToMakeBackup(day, week, month int) bool {

	if day > 0 ||
		(week > 0 && GetDateTimeNow("dow") == WeeklyBackupDay) ||
		(month > 0 && GetDateTimeNow("dom") == MonthlyBackupDay) {
		return true
	}

	return false
}

func GetBackupFullPath(dirPath, baseName, baseExtension, prefix string, gZip bool) (fullPath string) {

	fileName := fmt.Sprintf("%s_%s.%s", baseName, GetDateTimeNow(""), baseExtension)

	if prefix != "" {
		fileName = fmt.Sprintf("%s-%s", prefix, baseName)
	}

	if gZip {
		fileName += ".gz"
	}

	fullPath = filepath.Join(dirPath, fileName)

	return fullPath
}

func BackupDelivery(appCtx *appctx.AppContext, ofs map[string]string, storages []interfaces.Storage) (errs []error) {

	for i, st := range storages {
		moveOfs := false
		if i == len(storages)-1 && st.IsLocal() == 1 {
			moveOfs = true
		}

		for o, filePath := range ofs {
			err := st.CopyFile(appCtx, filePath, o, moveOfs)
			if err != nil {
				errs = append(errs, err)
			}
		}
	}
	return
}

// Contains checks if a string is present in a slice
func Contains(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}

	return false
}

func GetDstAndLinks(bakFile, ofs, bakPath string, days, weeks, months int) (dst string, links map[string]string, err error) {

	var rel string
	links = make(map[string]string)

	if GetDateTimeNow("dom") == MonthlyBackupDay && months > 0 {
		dstPath := path.Join(bakPath, ofs, "monthly")
		dst = path.Join(dstPath, bakFile)
	}
	if GetDateTimeNow("dow") == WeeklyBackupDay && weeks > 0 {
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
