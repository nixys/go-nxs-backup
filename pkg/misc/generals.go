package misc

import (
	"compress/gzip"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	monthlyBackupDay = "1"
	weeklyBackupDay  = "7"
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

var AllowedStorageConnectParams = []string{
	"s3_params",
	"scp_params",
	"sftp_params",
	"ftp_params",
	"smb_params",
	"nfs_params",
	"webdav_params",
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

func GetNeedToMakeBackup(day, week, month int) bool {

	if day > 0 ||
		(week > 0 && GetDateTimeNow("dow") == weeklyBackupDay) ||
		(month > 0 && GetDateTimeNow("dom") == monthlyBackupDay) {
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

	if GetDateTimeNow("dom") == monthlyBackupDay && months > 0 {
		dstPath := path.Join(bakPath, ofs, "monthly")
		dst = path.Join(dstPath, bakFile)
	}
	if GetDateTimeNow("dow") == weeklyBackupDay && weeks > 0 {
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

	if GetDateTimeNow("dom") == monthlyBackupDay && months > 0 {
		dst = append(dst, path.Join(basePath, "monthly", bakFile))
	}
	if GetDateTimeNow("dow") == weeklyBackupDay && weeks > 0 {
		dst = append(dst, path.Join(basePath, "weekly", bakFile))
	}
	if days > 0 {
		dst = append(dst, path.Join(basePath, "daily", bakFile))
	}

	return
}

func GetBackupWriter(filePath string, gZip bool) (io.WriteCloser, error) {
	backupFile, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}

	var writer io.WriteCloser
	if gZip {
		writer = gzip.NewWriter(backupFile)
	} else {
		writer = backupFile
	}

	return writer, nil
}

// RandString generates random string
func RandString(strLen int64) string {

	var chars = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	rand.Seed(time.Now().UnixNano())

	b := make([]rune, strLen)

	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}

	return string(b)
}
