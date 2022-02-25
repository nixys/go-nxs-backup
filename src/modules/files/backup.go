package files

import (
	appctx "github.com/nixys/nxs-go-appctx/v2"
	"nxs-backup/ctx"
)

func MakeBackup(appCtx *appctx.AppContext) []error {

	var errs []error

	cc := appCtx.CustomCtx().(*ctx.Ctx)

	for _, job := range cc.Conf.FilesJobs {
		switch job.JobType {
		case "desc_files":
			err := makeDescBackup(appCtx, job)
			if err != nil {
				errs = append(errs, err...)
			}
		case "inc_files":
			err := makeIncBackup(appCtx, job)
			if err != nil {
				errs = append(errs, err)
			}
		}
	}

	return errs
}
