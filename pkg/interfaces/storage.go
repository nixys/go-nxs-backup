package interfaces

import (
	"io/fs"
	"nxs-backup/misc"
	"os"
	"path"

	appctx "github.com/nixys/nxs-go-appctx/v2"

	"nxs-backup/modules/storage"
)

type Storage interface {
	IsLocal() int
	SetBackupPath(path string)
	SetRetention(r storage.Retention)
	DeliveryBackup(appCtx *appctx.AppContext, tmpBackupPath, ofs, bakType string) error
	DeleteOldBackups(appCtx *appctx.AppContext, ofsPartsList []string, bakType string) error
	GetFile(path string) (fs.File, error)
	Close() error
	Clone() Storage
}

type Storages []Storage

func (s Storages) Len() int           { return len(s) }
func (s Storages) Less(i, j int) bool { return s[i].IsLocal() < s[j].IsLocal() }
func (s Storages) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func (s Storages) DeleteOldBackups(appCtx *appctx.AppContext, j Job) (errs []error) {
	for _, st := range s {
		err := st.DeleteOldBackups(appCtx, j.GetTargetOfsList(), j.GetType())
		if err != nil {
			errs = append(errs, err)
		}
	}
	return
}

func (s Storages) Delivery(appCtx *appctx.AppContext, job Job) (errs []error) {

	for ofs, tmpBackupFile := range job.GetDumpedObjects() {

		for _, st := range s {
			if err := st.DeliveryBackup(appCtx, tmpBackupFile, ofs, job.GetType()); err != nil {
				errs = append(errs, err)
			}
		}

		if job.GetType() == misc.IncBackupType {
			// cleanup tmp metadata files
			_ = os.Remove(path.Join(path.Dir(tmpBackupFile), path.Base(tmpBackupFile)+".inc"))
			initFile := path.Join(path.Dir(tmpBackupFile), path.Base(tmpBackupFile)+".init")
			if _, err := os.Stat(initFile); err == nil {
				_ = os.Remove(initFile)
			}
		}

		// cleanup tmp backup file
		if err := os.Remove(tmpBackupFile); err != nil {
			errs = append(errs, err)
		}
		appCtx.Log().Infof("deleted temp backup file '%s'", tmpBackupFile)
	}
	return
}

func (s Storages) Close() error {
	for _, st := range s {
		_ = st.Close()
	}
	return nil
}
