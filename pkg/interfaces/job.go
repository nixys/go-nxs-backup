package interfaces

import appctx "github.com/nixys/nxs-go-appctx/v2"

type Job interface {
	GetName() string
	GetTempDir() string
	GetType() string
	IsBackupSafety() bool
	IsNeedToMakeBackup() bool
	DoBackup(ctx *appctx.AppContext, tmpDir string) []error
	CleanupOldBackups(ctx *appctx.AppContext) []error
}
