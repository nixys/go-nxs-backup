package local

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"nxs-backup/misc"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	appctx "github.com/nixys/nxs-go-appctx/v2"

	"nxs-backup/interfaces"
	. "nxs-backup/modules/storage"
)

type Local struct {
	BackupPath string
	Retention
}

func Init() *Local {
	return &Local{}
}

func (l *Local) IsLocal() int { return 1 }

func (l *Local) SetBackupPath(path string) {
	l.BackupPath = path
}

func (l *Local) SetRetention(r Retention) {
	l.Retention = r
}

func (l *Local) DeliveryBackup(appCtx *appctx.AppContext, tmpBackupFile, ofs, bakType string) (err error) {
	var (
		bakDstPath, mtdDstPath string
		links                  map[string]string
	)

	if bakType == misc.IncBackupType {
		bakDstPath, mtdDstPath, links, err = GetIncBackupDstAndLinks(tmpBackupFile, ofs, l.BackupPath)
		if err != nil {
			return
		}
		if err = l.deliveryBackupMetadata(appCtx, tmpBackupFile, mtdDstPath); err != nil {
			return
		}
	} else {
		bakDstPath, links, err = GetDescBackupDstAndLinks(tmpBackupFile, ofs, l.BackupPath, l.Retention)
		if err != nil {
			return
		}
	}

	err = os.MkdirAll(path.Dir(bakDstPath), os.ModePerm)
	if err != nil {
		appCtx.Log().Errorf("Unable to create directory: '%s'", err)
		return err
	}

	bakDst, err := os.Create(bakDstPath)
	if err != nil {
		return
	}
	defer func() { _ = bakDst.Close() }()

	bakSrc, err := os.Open(tmpBackupFile)
	if err != nil {
		return
	}
	defer func() { _ = bakSrc.Close() }()

	_, err = io.Copy(bakDst, bakSrc)
	if err != nil {
		appCtx.Log().Errorf("Unable to make copy: %s", err)
		return
	}
	appCtx.Log().Infof("Successfully copied temp backup to %s", bakDstPath)

	for dst, src := range links {
		err = os.MkdirAll(path.Dir(dst), os.ModePerm)
		if err != nil {
			appCtx.Log().Errorf("Unable to create directory: '%s'", err)
			return err
		}
		err = os.Symlink(src, dst)
		if err != nil {
			return err
		}
		appCtx.Log().Infof("Successfully created symlink %s", dst)
	}

	return
}

func (l *Local) deliveryBackupMetadata(appCtx *appctx.AppContext, tmpBackupFile, mtdDstPath string) error {
	mtdSrcPath := tmpBackupFile + ".inc"

	mtdSrc, err := os.Open(mtdSrcPath)
	if err != nil {
		return err
	}
	defer func() { _ = mtdSrc.Close() }()

	err = os.MkdirAll(path.Dir(mtdDstPath), os.ModePerm)
	if err != nil {
		appCtx.Log().Errorf("Unable to create directory: '%s'", err)
		return err
	}

	mtdDst, err := os.Create(mtdDstPath)
	if err != nil {
		return err
	}
	defer func() { _ = mtdDst.Close() }()

	_, err = io.Copy(mtdDst, mtdSrc)
	if err != nil {
		appCtx.Log().Errorf("Unable to make copy: %s", err)
		return err
	}
	appCtx.Log().Infof("Successfully copied metadata to %s", mtdDstPath)

	return nil
}

func (l *Local) DeleteOldBackups(appCtx *appctx.AppContext, ofsPartsList []string, bakType string) error {

	var errs []error
	curDate := time.Now()

	for _, ofsPart := range ofsPartsList {
		if bakType == misc.IncBackupType {
			intMoy, _ := strconv.Atoi(misc.GetDateTimeNow("moy"))
			lastMonth := intMoy - l.Months

			var year string
			if lastMonth > 0 {
				year = misc.GetDateTimeNow("year")
			} else {
				year = misc.GetDateTimeNow("previous_year")
				lastMonth += 12
			}

			backupDir := path.Join(l.BackupPath, ofsPart, year)
			dirs, err := ioutil.ReadDir(backupDir)
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					return nil
				} else {
					appCtx.Log().Errorf("Failed to get access to directory '%s' with next error: %v", backupDir, err)
					return err
				}
			}

			for _, dir := range dirs {
				rx := regexp.MustCompile("month_\\d\\d")
				if rx.MatchString(dir.Name()) {
					dirParts := strings.Split(dir.Name(), "_")
					dirMonth, _ := strconv.Atoi(dirParts[1])
					if dirMonth < lastMonth {
						err = os.RemoveAll(path.Join(backupDir, dir.Name()))
						if err != nil {
							appCtx.Log().Errorf("Failed to delete '%s' in dir '%s' with next error: %s", dir.Name(), backupDir, err)
							errs = append(errs, err)
						}
					}
				}
			}
		} else {
			for _, period := range []string{"daily", "weekly", "monthly"} {
				backupDir := path.Join(l.BackupPath, ofsPart, period)
				files, err := ioutil.ReadDir(backupDir)
				if err != nil {
					if errors.Is(err, fs.ErrNotExist) {
						continue
					}
					appCtx.Log().Errorf("Failed to read files in directory '%s' with next error: %s", backupDir, err)
					return err
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
						err = os.Remove(path.Join(backupDir, file.Name()))
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
	}

	if len(errs) > 0 {
		return fmt.Errorf("some errors on file deletion")
	}

	return nil
}

func (l *Local) GetFile(ofsPath string) (fs.File, error) {
	fp, err := filepath.EvalSymlinks(path.Join(l.BackupPath, ofsPath))
	if err != nil {
		return nil, err
	}
	return os.Open(fp)
}

func (l *Local) Close() error {
	return nil
}

func (l *Local) Clone() interfaces.Storage {
	cl := *l
	return &cl
}
