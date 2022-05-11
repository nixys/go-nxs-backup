package interfaces

import appctx "github.com/nixys/nxs-go-appctx/v2"

type Storage interface {
	CopyFile(appCtx *appctx.AppContext, tmpBackupPath, ofs string, move bool) error
	ListFiles() error
	ControlFiles(appCtx *appctx.AppContext, ofsPartsList []string) []error
	IsLocal() int
}

type StorageSortByLocal []Storage

func (s StorageSortByLocal) Len() int           { return len(s) }
func (s StorageSortByLocal) Less(i, j int) bool { return s[i].IsLocal() < s[j].IsLocal() }
func (s StorageSortByLocal) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
