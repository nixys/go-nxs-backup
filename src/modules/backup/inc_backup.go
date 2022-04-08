package backup

import (
	appctx "github.com/nixys/nxs-go-appctx/v2"
)

type IncFilesJob struct {
	JobName string
}

func (j IncFilesJob) GetJobType() string {
	return "inc_files"
}

func (j IncFilesJob) DoBackup(appCtx *appctx.AppContext) (errs []error) {
	return
}
