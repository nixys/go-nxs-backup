package interfaces

import appctx "github.com/nixys/nxs-go-appctx/v2"

type Job interface {
	JobName() string
	JobType() string
	DoBackup(appCtx *appctx.AppContext) []error
}
