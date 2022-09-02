package interfaces

import appctx "github.com/nixys/nxs-go-appctx/v2"

type Job interface {
	GetName() string
	GetTempDir() string
	GetType() string
	GetTargetOfsList() []string
	GetStoragesCount() int
	GetDumpObjects() map[string]DumpObject
	SetDumpObjectDelivered(ofs string)
	IsBackupSafety() bool
	NeedToMakeBackup() bool
	NeedToUpdateIncMeta() bool
	DoBackup(ctx *appctx.AppContext, tmpDir string) []error
	DeleteOldBackups(ctx *appctx.AppContext, full bool) []error
	CleanupTmpData(ctx *appctx.AppContext) error
	Close() error
}

type Jobs []Job

func (j Jobs) Close() error {
	for _, job := range j {
		_ = job.Close()
	}
	return nil
}

type DumpObject struct {
	TmpFile   string
	Delivered bool
}
