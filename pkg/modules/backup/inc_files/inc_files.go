package inc_files

import (
	appctx "github.com/nixys/nxs-go-appctx/v2"

	"nxs-backup/interfaces"
)

type Job struct {
	Name     string
	storages interfaces.Storages
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

func (j *Job) Close() error {
	for _, st := range j.storages {
		_ = st.Close()
	}
	return nil
}
