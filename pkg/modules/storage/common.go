package storage

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
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

func GetDescBackupDstAndLinks(tmpBackupFile, ofs, bakPath string, retention Retention) (dst string, links map[string]string, err error) {

	var relative string
	links = make(map[string]string)

	bakFileName := path.Base(tmpBackupFile)

	if misc.GetDateTimeNow("dom") == misc.MonthlyBackupDay && retention.Months > 0 {
		dst = path.Join(bakPath, ofs, "monthly", bakFileName)
	}
	if misc.GetDateTimeNow("dow") == misc.WeeklyBackupDay && retention.Weeks > 0 {
		dstPath := path.Join(bakPath, ofs, "weekly")
		if dst != "" {
			relative, err = filepath.Rel(dstPath, dst)
			if err != nil {
				return
			}
			links[path.Join(dstPath, bakFileName)] = relative
		} else {
			dst = path.Join(dstPath, bakFileName)
		}
	}
	if retention.Days > 0 {
		dstPath := path.Join(bakPath, ofs, "daily")
		if dst != "" {
			relative, err = filepath.Rel(dstPath, dst)
			if err != nil {
				return
			}
			links[path.Join(dstPath, bakFileName)] = relative
		} else {
			dst = path.Join(dstPath, bakFileName)
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

func GetIncBackupDstAndLinks(tmpBackupFile, ofs, bakPath string) (bakDst, mtdDst string, links map[string]string, err error) {

	var decadeDay, relative string
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

	init := true
	if _, err = os.Stat(tmpBackupFile + ".init"); errors.Is(err, fs.ErrNotExist) {
		init = false
		err = nil
	}

	bakFileName := path.Base(tmpBackupFile)
	bakBasePath := path.Join(bakPath, ofs, year)
	mtdPath := path.Join(bakBasePath, "inc_meta_info")

	if misc.GetDateTimeNow("doy") == misc.YearlyBackupDay || init {
		bakDst = path.Join(bakBasePath, "year", bakFileName)
		mtdDst = path.Join(mtdPath, "year.inc")
	}

	if dom == misc.MonthlyBackupDay || init {
		monthBakDst := path.Join(bakBasePath, month, "monthly")
		if bakDst != "" {
			relative, err = filepath.Rel(monthBakDst, bakDst)
			if err != nil {
				return
			}
			links[path.Join(monthBakDst, bakFileName)] = relative
		} else {
			bakDst = path.Join(monthBakDst, bakFileName)
		}
		monthMtdDst := path.Join(mtdPath, "month.inc")
		if mtdDst != "" {
			relative, err = filepath.Rel(mtdPath, mtdDst)
			if err != nil {
				return
			}
			links[monthMtdDst] = relative
		} else {
			mtdDst = monthMtdDst
		}
	}

	dayDstPath := path.Join(bakBasePath, month, decadeDay)
	if bakDst != "" {
		relative, err = filepath.Rel(dayDstPath, bakDst)
		if err != nil {
			return
		}
		links[path.Join(dayDstPath, bakFileName)] = relative
	} else {
		bakDst = path.Join(dayDstPath, bakFileName)
	}
	if misc.Contains(misc.DecadesBackupDays, dom) || init {
		dayDst := path.Join(mtdPath, "day.inc")
		if mtdDst != "" {
			relative, err = filepath.Rel(mtdPath, mtdDst)
			if err != nil {
				return
			}
			links[dayDst] = relative
		} else {
			mtdDst = dayDst
		}
	}

	return
}
