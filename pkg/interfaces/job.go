package interfaces

import appctx "github.com/nixys/nxs-go-appctx/v2"

type Job interface {
	GetName() string
	GetTempDir() string
	GetType() string
	GetTargetOfsList() []string
	GetStoragesCount() int
	GetDumpedObjects() map[string]string
	IsBackupSafety() bool
	NeedToMakeBackup() bool
	NeedToUpdateIncMeta() bool
	DoBackup(ctx *appctx.AppContext, tmpDir string) []error
	DeleteOldBackups(ctx *appctx.AppContext) []error
	Close() error
}

type Jobs []Job

func (j Jobs) Close() error {
	for _, job := range j {
		_ = job.Close()
	}
	return nil
}
