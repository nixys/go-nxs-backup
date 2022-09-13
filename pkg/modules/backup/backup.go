package backup

import (
	"fmt"
	"github.com/hashicorp/go-multierror"
	"os"
	"path"
	"path/filepath"

	appctx "github.com/nixys/nxs-go-appctx/v2"

	"nxs-backup/interfaces"
	"nxs-backup/misc"
)

func Perform(appCtx *appctx.AppContext, job interfaces.Job) error {
	var errs *multierror.Error

	if job.GetStoragesCount() == 0 {
		appCtx.Log().Warn("There are no configured storages for job.")
		return nil
	}

	if !job.IsBackupSafety() {
		if err := job.DeleteOldBackups(appCtx, ""); err != nil {
			errs = multierror.Append(errs, err)
		}
	} else {
		defer func() {
			err := job.DeleteOldBackups(appCtx, "")
			if err != nil {
				errs = multierror.Append(errs, err)
			}
		}()
	}

	if !job.NeedToMakeBackup() {
		appCtx.Log().Infof("According to the backup plan today new backups are not created for job %s", job.GetName())
		return nil
	}

	appCtx.Log().Infof("Starting job %s", job.GetName())

	tmpDirPath := path.Join(job.GetTempDir(), fmt.Sprintf("%s_%s", job.GetType(), misc.GetDateTimeNow("")))
	err := os.MkdirAll(tmpDirPath, os.ModePerm)
	if err != nil {
		appCtx.Log().Errorf("Job `%s` failed. Unable to create tmp dir with next error: %s", job.GetName(), err)
		errs = multierror.Append(errs, err)
		return errs.ErrorOrNil()
	}

	if err = job.DoBackup(appCtx, tmpDirPath); err != nil {
		errs = multierror.Append(errs, err)
	}

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

	return errs.ErrorOrNil()
}
