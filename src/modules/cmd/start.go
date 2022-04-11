package cmd

import (
	"fmt"
	"strings"

	appctx "github.com/nixys/nxs-go-appctx/v2"

	"nxs-backup/ctx"
	"nxs-backup/ctx/args"
)

func Start(appCtx *appctx.AppContext) error {

	var errs []string

	cc := appCtx.CustomCtx().(*ctx.Ctx)

	for _, job := range cc.Jobs {

		switch cc.Args.Values.(args.StartOpts).JobName {
		case "all":
			appCtx.Log().Info("Starting backup all jobs.")
			errList := job.DoBackup(appCtx)
			if len(errList) > 0 {
				for _, err := range errList {
					errs = append(errs, err.Error())
				}
			}
		case "databases":
			appCtx.Log().Info("Starting backup databases jobs.")
		case "files":
			appCtx.Log().Info("Starting backup files jobs.")
			errList := job.DoBackup(appCtx)
			if len(errList) > 0 {
				for _, err := range errList {
					errs = append(errs, err.Error())
				}
			}
		case "external":
			if job.GetJobType() == "external" {
				appCtx.Log().Info("Starting backup external jobs.")
			}
		default:
			appCtx.Log().Info("Starting backup 'some' jobs.")
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("Some of backups failed with next errors:\n%s", strings.Join(errs, "\n"))
	}
	return nil
}
