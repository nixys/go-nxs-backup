package inc_files

import (
	appctx "github.com/nixys/nxs-go-appctx/v2"
)

type Job struct {
	Name string
}

func (j Job) GetJobName() string {
	return j.Name
}

func (j Job) GetJobType() string {
	return "files"
}

func (j Job) DoBackup(appCtx *appctx.AppContext) (errs []error) {
	return
}
