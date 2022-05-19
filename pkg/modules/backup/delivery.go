package backup

import (
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
	}
	return
}
