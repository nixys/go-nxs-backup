package backup

import (
	"os"

	appctx "github.com/nixys/nxs-go-appctx/v2"

	"nxs-backup/interfaces"
)

type DumpedObjects map[string]string

func (o DumpedObjects) Delivery(appCtx *appctx.AppContext, storages []interfaces.Storage) (errs []error) {

	for obj, tmpBackupPath := range o {
		for i, st := range storages {
			moveOfs := false
			if i == len(storages)-1 && st.IsLocal() == 1 {
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
			err = os.Remove(tmpBackupPath)
			if err != nil {
				errs = append(errs, err)
			}
			appCtx.Log().Infof("deleted temp file '%s'", tmpBackupPath)
		}
	}
	return
}
