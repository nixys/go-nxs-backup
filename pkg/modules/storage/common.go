package storage

import (
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"strconv"
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

func GetNeedToMakeBackup(day, week, month int) bool {

	if day > 0 ||
		(week > 0 && misc.GetDateTimeNow("dow") == misc.WeeklyBackupDay) ||
		(month > 0 && misc.GetDateTimeNow("dom") == misc.MonthlyBackupDay) {
		return true
	}

	return false
}

func GetDescBackupDstAndLinks(bakFile, ofs, bakPath string, retention Retention) (dst string, links map[string]string, err error) {

	var relative string
	links = make(map[string]string)

	if misc.GetDateTimeNow("dom") == misc.MonthlyBackupDay && retention.Months > 0 {
		dst = path.Join(bakPath, ofs, "monthly", bakFile)
	}
	if misc.GetDateTimeNow("dow") == misc.WeeklyBackupDay && retention.Weeks > 0 {
		dstPath := path.Join(bakPath, ofs, "weekly")
		if dst != "" {
			relative, err = filepath.Rel(dstPath, dst)
			if err != nil {
				return
			}
			links[path.Join(dstPath, bakFile)] = relative
		} else {
			dst = path.Join(dstPath, bakFile)
		}
	}
	if retention.Days > 0 {
		dstPath := path.Join(bakPath, ofs, "daily")
		if dst != "" {
			relative, err = filepath.Rel(dstPath, dst)
			if err != nil {
				return
			}
			links[path.Join(dstPath, bakFile)] = relative
		} else {
			dst = path.Join(dstPath, bakFile)
		}
	}

	return
}

func GetDescBackupDstList(bakFile, ofs, bakPath string, retention Retention) (dst []string) {

	basePath := strings.TrimPrefix(path.Join(bakPath, ofs), "/")

	if misc.GetDateTimeNow("dom") == misc.MonthlyBackupDay && retention.Months > 0 {
		dst = append(dst, path.Join(basePath, "monthly", bakFile))
	}
	if misc.GetDateTimeNow("dow") == misc.WeeklyBackupDay && retention.Weeks > 0 {
		dst = append(dst, path.Join(basePath, "weekly", bakFile))
	}
	if retention.Days > 0 {
		dst = append(dst, path.Join(basePath, "daily", bakFile))
	}

	return
}

func GetIncBackupDstAndLinks(bakFile, ofs, bakPath string, init bool) (dst string, links map[string]string, err error) {

	var relative, decadeDay string
	links = make(map[string]string)

	year := misc.GetDateTimeNow("year")
	dom := misc.GetDateTimeNow("dom")
	month := fmt.Sprintf("month_%02s", misc.GetDateTimeNow("moy"))
	intDom, _ := strconv.Atoi(dom)
	if intDom < 11 {
		decadeDay = "day_01"
	} else if intDom > 20 {
		decadeDay = "day_21"
	} else {
		decadeDay = "day_11"
	}

	basePath := path.Join(bakPath, ofs, year)

	if misc.GetDateTimeNow("doy") == misc.YearlyBackupDay || init {
		dst = path.Join(basePath, "year", bakFile)
	}

	if dom == misc.MonthlyBackupDay || init {
		montDstPath := path.Join(basePath, month, "monthly")
		if dst != "" {
			relative, err = filepath.Rel(montDstPath, dst)
			if err != nil {
				return
			}
			links[path.Join(montDstPath, bakFile)] = relative
		} else {
			dst = path.Join(montDstPath, bakFile)
		}
	}

	dayDstPath := path.Join(basePath, month, decadeDay)
	if dst != "" {
		relative, err = filepath.Rel(dayDstPath, dst)
		if err != nil {
			return
		}
		links[path.Join(dayDstPath, bakFile)] = relative
	} else {
		dst = path.Join(dayDstPath, bakFile)
	}

	return
}

func GetIncMetaDstAndLinks(ofs, bakPath string, init bool) (dst string, links map[string]string, err error) {

	var relative string
	links = make(map[string]string)

	year := misc.GetDateTimeNow("year")
	dom := misc.GetDateTimeNow("dom")

	metadataPath := path.Join(bakPath, ofs, year, "inc_meta_info")

	//yearDst := path.Join(metadataPath, "year.inc")

	if misc.GetDateTimeNow("doy") == misc.YearlyBackupDay || init {
		dst = path.Join(metadataPath, "year.inc")
	}

	if dom == misc.MonthlyBackupDay || init {
		montDst := path.Join(metadataPath, "month.inc")
		if dst != "" {
			relative, err = filepath.Rel(metadataPath, dst)
			if err != nil {
				return
			}
			links[montDst] = relative
		} else {
			dst = montDst
		}
	}

	if misc.Contains(misc.DecadesBackupDays, dom) || init {
		dayDst := path.Join(metadataPath, "day.inc")
		if dst != "" {
			relative, err = filepath.Rel(metadataPath, dst)
			if err != nil {
				return
			}
			links[dayDst] = relative
		} else {
			dst = dayDst
		}
	}

	return
}
