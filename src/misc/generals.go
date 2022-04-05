package misc

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"nxs-backup/interfaces"
)

const (
	MonthlyBackupDay = "1"
	WeeklyBackupDay  = "7"
)

func GetBackupFileName(regex, target string) string {
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

func GetSubPath() (res string) {
	if GetDateTimeNow("dom") == MonthlyBackupDay {
		return "monthly"
	} else if GetDateTimeNow("dow") == WeeklyBackupDay {
		return "weekly"
	}
	return "daily"
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

func BackupDelivery(ofs map[string]string, storages []interfaces.Storage) (errs []error) {

	for i, st := range storages {
		moveOfs := false
		if i == len(storages)-1 && st.IsLocal() == 1 {
			moveOfs = true
		}

		for o, filePath := range ofs {
			err := st.CopyFile(filePath, o, moveOfs)
			if err != nil {
				errs = append(errs, err)
			}
		}
	}
	return
}
