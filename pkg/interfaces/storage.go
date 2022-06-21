package interfaces

import (
	appctx "github.com/nixys/nxs-go-appctx/v2"

	"nxs-backup/modules/storage"
)

type Storage interface {
	IsLocal() int
	SetBackupPath(path string)
	SetRetention(r storage.Retention)
	CopyFile(appCtx *appctx.AppContext, tmpBackupPath, ofs string, move bool) error
	ControlFiles(appCtx *appctx.AppContext, ofsPartsList []string) []error
}

type StorageSortByLocal []Storage

func (s StorageSortByLocal) Len() int           { return len(s) }
func (s StorageSortByLocal) Less(i, j int) bool { return s[i].IsLocal() < s[j].IsLocal() }
func (s StorageSortByLocal) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
