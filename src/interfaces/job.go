package interfaces

import appctx "github.com/nixys/nxs-go-appctx/v2"

type Job interface {
	GetJobType() string
	DoBackup(appCtx *appctx.AppContext) []error
}
