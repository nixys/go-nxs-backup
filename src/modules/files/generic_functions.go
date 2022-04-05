package files

import (
	"nxs-backup/interfaces"
)

func BackupDelivery(ofs map[string]string, storages []interfaces.Storage) (errs []error) {

	for i, st := range storages {
		moveOfs := false
		if i == len(storages)-1 && st.IsLocal() == 1 {
			moveOfs = true
		}

		for o, filePath := range ofs {
			err := st.CopyFile(filePath, o, moveOfs)
			if err != nil {
				errs = append(errs, err)
			}
		}
	}
	return
}
