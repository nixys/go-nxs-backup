package storage

import (
	"io"
	"io/ioutil"
	"nxs-backup/misc"
	"os"
	"path/filepath"
	"time"

	appctx "github.com/nixys/nxs-go-appctx/v2"
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

func (l *Local) IsLocal() int { return 1 }

func (l *Local) CopyFile(appCtx *appctx.AppContext, tmpBackup, ofs string, move bool) (err error) {

	source, err := os.Open(tmpBackup)
	if err != nil {
		return
	}
	defer source.Close()

	dstPath, links, err := misc.GetDstAndLinks(filepath.Base(tmpBackup), ofs, l.BackupPath, l.Days, l.Weeks, l.Months)
	if err != nil {
		return
	}

	err = os.MkdirAll(filepath.Dir(dstPath), os.ModePerm)
	if err != nil {
		appCtx.Log().Errorf("Unable to create directory: '%s'", err)
		return err
	}

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
		err = os.MkdirAll(filepath.Dir(dst), os.ModePerm)
		if err != nil {
			appCtx.Log().Errorf("Unable to create directory: '%s'", err)
			return err
		}
		err = os.Symlink(src, dst)
		if err != nil {
			return err
		}
	}

	if move {
		err = os.Remove(tmpBackup)
		appCtx.Log().Infof("Successfully moved file '%s' to %s", source.Name(), dstPath)
	} else {
		appCtx.Log().Infof("Successfully copied file '%s' to %s", source.Name(), dstPath)
	}

	return
}

func (l *Local) ListFiles() (err error) {
	return
}

func (l *Local) ControlFiles(appCtx *appctx.AppContext, ofsPartsList []string) (errs []error) {

	curDate := time.Now()

	for _, period := range []string{"daily", "weekly", "monthly"} {
		for _, ofsPart := range ofsPartsList {
			backupDir := filepath.Join(l.BackupPath, ofsPart, period)
			files, err := ioutil.ReadDir(backupDir)
			if err != nil {
				if os.IsNotExist(err) {
					appCtx.Log().Warnf("Error: %s", err)
					continue
				}
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

				retentionDate = retentionDate.Truncate(24 * time.Hour)
				if curDate.After(retentionDate) {
					err = os.Remove(filepath.Join(backupDir, file.Name()))
					if err != nil {
						appCtx.Log().Errorf("Failed to delete file '%s' in directory '%s' with next error: %s",
							file.Name(), backupDir, err)
						errs = append(errs, err)
					} else {
						appCtx.Log().Infof("Successfully deleted old backup file '%s' in directory '%s'", file.Name(), backupDir)
					}
				}
			}
		}
	}
	return
}
