package backup

import (
	"fmt"
	appctx "github.com/nixys/nxs-go-appctx/v2"
	"os"
	"path"
	"path/filepath"

	"nxs-backup/interfaces"
	"nxs-backup/misc"
)

func Perform(appCtx *appctx.AppContext, job interfaces.Job) (errs []error) {

	if job.GetStoragesCount() == 0 {
		appCtx.Log().Warn("There are no configured storages for job.")
		return
	}

	if !job.IsBackupSafety() {
		errs = job.DeleteOldBackups(appCtx, false)
	} else {
		defer func() {
			err := job.DeleteOldBackups(appCtx, false)
			if err != nil {
				errs = append(errs, err...)
			}
		}()
	}

	if !job.NeedToMakeBackup() {
		appCtx.Log().Infof("According to the backup plan today new backups are not created for job %s", job.GetName())
		return
	}

	appCtx.Log().Infof("Starting job %s", job.GetName())

	tmpDirPath := path.Join(job.GetTempDir(), fmt.Sprintf("%s_%s", job.GetType(), misc.GetDateTimeNow("")))
	err := os.MkdirAll(tmpDirPath, os.ModePerm)
	if err != nil {
		appCtx.Log().Errorf("Job `%s` failed. Unable to create tmp dir with next error: %s", job.GetName(), err)
		return []error{err}
	}

	errList := job.DoBackup(appCtx, tmpDirPath)
	errs = append(errs, errList...)

	err = job.CleanupTmpData(appCtx)

	err = filepath.Walk(tmpDirPath,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			// try to delete empty dirs
			if info.IsDir() {
				_ = os.Remove(path)
			}
			return nil
		})
	// cleanup tmp dir
	_ = os.Remove(tmpDirPath)

	return
}
