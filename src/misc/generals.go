package misc

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"
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

func GetFullPath(dirPath, baseName, baseExtension, prefix string, gZip bool) (fullPath string) {

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
