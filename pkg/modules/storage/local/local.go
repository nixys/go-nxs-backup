package local

import (
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
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

func (l *Local) DeliveryDescBackup(appCtx *appctx.AppContext, tmpBackup, ofs string) (err error) {
	var (
		dstPath string
		links   map[string]string
	)

	source, err := os.Open(tmpBackup)
	if err != nil {
		return
	}
	defer source.Close()

	dstPath, links, err = GetDescBackupDstAndLinks(path.Base(tmpBackup), ofs, l.BackupPath, l.Retention)
	if err != nil {
		return
	}

	err = os.MkdirAll(path.Dir(dstPath), os.ModePerm)
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
		appCtx.Log().Errorf("Unable to make copy: %s", err)
		return
	}
	appCtx.Log().Infof("Successfully copied temp backup to %s", dstPath)

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

func (l *Local) DeliveryIncBackup(appCtx *appctx.AppContext, tmpBackup, ofs string, init bool) (err error) {
	var (
		dstPath string
		links   map[string]string
	)

	source, err := os.Open(tmpBackup)
	if err != nil {
		return
	}
	defer source.Close()

	dstPath, links, err = GetIncBackupDstAndLinks(path.Base(tmpBackup), ofs, l.BackupPath, init)
	if err != nil {
		return
	}

	err = os.MkdirAll(path.Dir(dstPath), os.ModePerm)
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
		appCtx.Log().Errorf("Unable to make copy: %s", err)
		return
	}
	appCtx.Log().Infof("Successfully copied temp backup to %s", dstPath)

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

func (l *Local) DeliveryIncBackupMetadata(appCtx *appctx.AppContext, tmpBackupMetadata, ofs string, init bool) (err error) {
	var (
		mtdDst      string
		mtdLinks    map[string]string
		destination *os.File
	)

	source, err := os.Open(tmpBackupMetadata)
	if err != nil {
		return
	}
	defer source.Close()

	mtdDst, mtdLinks, err = GetIncMetaDstAndLinks(ofs, l.BackupPath, init)
	if err != nil {
		return
	}

	if mtdDst != "" {
		err = os.MkdirAll(path.Dir(mtdDst), os.ModePerm)
		if err != nil {
			appCtx.Log().Errorf("Unable to create directory: '%s'", err)
			return err
		}

		destination, err = os.Create(mtdDst)
		if err != nil {
			return
		}
		defer destination.Close()

		_, err = io.Copy(destination, source)
		if err != nil {
			appCtx.Log().Errorf("Unable to make copy: %s", err)
			return
		}
		appCtx.Log().Infof("Successfully copied metadata to %s", mtdDst)

		for dst, src := range mtdLinks {
			err = os.Symlink(src, dst)
			if err != nil {
				return err
			}
			appCtx.Log().Infof("Successfully created symlink %s", dst)
		}
	}

	return
}

func (l *Local) DeleteOldDescBackups(appCtx *appctx.AppContext, ofsPartsList []string) error {

	var errs []error
	curDate := time.Now()

	for _, period := range []string{"daily", "weekly", "monthly"} {
		for _, ofsPart := range ofsPartsList {
			backupDir := path.Join(l.BackupPath, ofsPart, period)
			files, err := ioutil.ReadDir(backupDir)
			if err != nil {
				if os.IsNotExist(err) {
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
