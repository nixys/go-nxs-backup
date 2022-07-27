package interfaces

import (
	appctx "github.com/nixys/nxs-go-appctx/v2"
	"os"

	"nxs-backup/modules/storage"
)

type Storage interface {
	IsLocal() int
	SetBackupPath(path string)
	SetRetention(r storage.Retention)
	CopyFile(appCtx *appctx.AppContext, tmpBackupPath, ofs string, move bool) error
	ControlFiles(appCtx *appctx.AppContext, ofsPartsList []string) error
	Close() error
	Clone() Storage
}

type Storages []Storage

func (s Storages) Len() int           { return len(s) }
func (s Storages) Less(i, j int) bool { return s[i].IsLocal() < s[j].IsLocal() }
func (s Storages) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func (s Storages) CleanupOldBackups(appCtx *appctx.AppContext, ofsPartsList []string) (errs []error) {
	for _, st := range s {
		err := st.ControlFiles(appCtx, ofsPartsList)
		if err != nil {
			errs = append(errs, err)
		}
	}
	return
}

func (s Storages) Delivery(appCtx *appctx.AppContext, dumpedObjects map[string]string) (errs []error) {

	for obj, tmpBackupPath := range dumpedObjects {
		for i, st := range s {
			moveOfs := false
			if i == len(s)-1 && st.IsLocal() == 1 {
				moveOfs = true
			}

			err := st.CopyFile(appCtx, tmpBackupPath, obj, moveOfs)
			if err != nil {
				errs = append(errs, err)
			}
		}
		_, err := os.Stat(tmpBackupPath)
		if err != nil {
			if !os.IsNotExist(err) {
				errs = append(errs, err)
			}
		} else {
			if err = os.Remove(tmpBackupPath); err != nil {
				errs = append(errs, err)
			}
			appCtx.Log().Infof("deleted temp backup file '%s'", tmpBackupPath)
		}
	}
	return
}

func (s Storages) Close() error {
	for _, st := range s {
		_ = st.Close()
	}
	return nil
}
