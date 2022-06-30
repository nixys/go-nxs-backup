package backup

import (
	"fmt"
	appctx "github.com/nixys/nxs-go-appctx/v2"
	"io/ioutil"
	"os"
	"path"

	"nxs-backup/interfaces"
	"nxs-backup/misc"
)

func Perform(appCtx *appctx.AppContext, job interfaces.Job) (errs []error) {

	if !job.IsBackupSafety() {
		errs = job.CleanupOldBackups(appCtx)
	} else {
		defer func() {
			err := job.CleanupOldBackups(appCtx)
			if err != nil {
				errs = append(errs, err...)
			}
		}()
	}

	if !job.IsNeedToMakeBackup() {
		appCtx.Log().Infof("According to the backup plan today new backups are not created for job %s", job.GetName())
		return
	}

	appCtx.Log().Infof("Starting job %s", job.GetName())

	tmpDirPath := path.Join(job.GetTempDir(), fmt.Sprintf("%s_%s", job.GetType(), misc.GetDateTimeNow("")))
	err := os.MkdirAll(tmpDirPath, os.ModePerm)
	if err != nil {
		appCtx.Log().Errorf("Failed to create tmp dir with next error: %s", err)
		return []error{err}
	}

	errList := job.DoBackup(appCtx, tmpDirPath)
	errs = append(errs, errList...)

	// cleanup tmp dir
	files, _ := ioutil.ReadDir(tmpDirPath)
	if len(files) == 0 {
		err = os.Remove(tmpDirPath)
		if err != nil {
			errs = append(errs, err)
		}
	}

	return
}
