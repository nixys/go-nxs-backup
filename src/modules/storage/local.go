package storage

import (
	appctx "github.com/nixys/nxs-go-appctx/v2"
	"io"
	"io/ioutil"
	"nxs-backup/misc"
	"os"
	"path"
	"path/filepath"
	"time"
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

func (l Local) ControlFiles(appCtx *appctx.AppContext, ofsPartsList []string) (errs []error) {

	curDate := time.Now()

	for _, period := range []string{"daily", "weekly", "monthly"} {
		for _, ofsPart := range ofsPartsList {
			backupDir := filepath.Join(l.BackupPath, ofsPart, period)
			files, err := ioutil.ReadDir(backupDir)
			if err != nil {
				appCtx.Log().Errorf("Failed to read files in directory '%s' with next error: %s", backupDir, err)
				return []error{err}
			}

			for _, file := range files {

				fileDate := file.ModTime()
				var retentionDate time.Time

				switch period {
				case "daily":
					retentionDate = fileDate.AddDate(0, 0, l.Retention.Days)
				case "weekly":
					retentionDate = fileDate.AddDate(0, 0, l.Retention.Weeks*7)
				case "monthly":
					retentionDate = fileDate.AddDate(0, l.Retention.Months, 0)
				}

				retentionDate = retentionDate.Round(24 * time.Hour)
				if curDate.After(retentionDate) {
					err = os.Remove(filepath.Join(backupDir, file.Name()))
					if err != nil {
						appCtx.Log().Errorf("Failed to delete file '%s' in directory '%s' with next error: %s",
							file.Name(), backupDir, err)
						errs = append(errs, err)
					} else {
						appCtx.Log().Infof("Successfully deleted file '%s' in directory '%s'", file.Name(), backupDir)
					}
				}
			}
		}
	}
	return
}
