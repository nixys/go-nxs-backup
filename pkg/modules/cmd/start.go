package cmd

import (
	"fmt"
	"strings"

	appctx "github.com/nixys/nxs-go-appctx/v2"

	"nxs-backup/ctx"
	"nxs-backup/ctx/args"
	"nxs-backup/modules/backup"
)

func Start(appCtx *appctx.AppContext) error {

	var (
		errs       []error
		errStrings []string
	)

	cc := appCtx.CustomCtx().(*ctx.Ctx)

	appCtx.Log().Info("Starting backup.")

	jobNameArg := cc.CmdParams.(*args.StartCmd).JobName

	if jobNameArg == "files" || jobNameArg == "all" {
		if len(cc.FilesJobs) > 0 {
			appCtx.Log().Info("Starting backup files jobs.")
			for _, job := range cc.FilesJobs {
				errList := backup.Perform(appCtx, job)
				errs = append(errs, errList...)
			}
		} else {
			appCtx.Log().Info("No files jobs.")
		}
	}
	if jobNameArg == "databases" || jobNameArg == "all" {
		if len(cc.DBsJobs) > 0 {
			appCtx.Log().Info("Starting backup databases jobs.")
			for _, job := range cc.DBsJobs {
				errList := backup.Perform(appCtx, job)
				errs = append(errs, errList...)
			}
		} else {
			appCtx.Log().Info("No databases jobs.")
		}
	}
	if jobNameArg == "external" || jobNameArg == "all" {
		if len(cc.ExternalJobs) > 0 {
			appCtx.Log().Info("Starting backup external jobs.")
			for _, job := range cc.ExternalJobs {
				errList := backup.Perform(appCtx, job)
				errs = append(errs, errList...)
			}
		} else {
			appCtx.Log().Info("No external jobs.")
		}
	}

	for _, job := range cc.Jobs {
		if job.GetName() == jobNameArg {
			errList := backup.Perform(appCtx, job)
			errs = append(errs, errList...)
		}
	}

	if len(errs) > 0 {
		for _, err := range errs {
			errStrings = append(errStrings, err.Error())
		}
		return fmt.Errorf("Some of backups failed with next errors:\n%s", strings.Join(errStrings, "\n"))
	}

	appCtx.Log().Info("Finished.")
	return nil
}
