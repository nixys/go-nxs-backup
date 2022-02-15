package cmd

import (
	"fmt"
	"strings"

	appctx "github.com/nixys/nxs-go-appctx/v2"

	"nxs-backup/ctx"
	"nxs-backup/ctx/args"
	"nxs-backup/modules/files_backup"
)

func Start(appCtx *appctx.AppContext) error {

	var errs []string

	cc := appCtx.CustomCtx().(*ctx.Ctx)

	a := cc.Args.Values.(args.StartOpts)

	switch a.JobName {
	case "all":
		errList := files_backup.MakeBackup(appCtx)
		if len(errList) > 0 {
			for _, err := range errList {
				errs = append(errs, err.Error())
			}
		}
	case "databases":
		fmt.Println("databases")
	case "files":
		errList := files_backup.MakeBackup(appCtx)
		if len(errList) > 0 {
			for _, err := range errList {
				errs = append(errs, err.Error())
			}
		}
	case "external":
		fmt.Println("external")
	default:
		fmt.Println("some_job")
	}

	if len(errs) > 0 {
		return fmt.Errorf("Some of backups failed with next errors:\n%s", strings.Join(errs, "\n"))
	}
	return nil
}
