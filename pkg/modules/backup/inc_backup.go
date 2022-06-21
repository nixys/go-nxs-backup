package backup

import (
	appctx "github.com/nixys/nxs-go-appctx/v2"
)

type IncFilesJob struct {
	Name string
}

func (j IncFilesJob) JobName() string {
	return j.Name
}

func (j IncFilesJob) JobType() string {
	return "inc_files"
}

func (j IncFilesJob) DoBackup(appCtx *appctx.AppContext) (errs []error) {
	return
}
