package interfaces

import (
	"errors"
	"io/fs"
	"os"
	"path"

	appctx "github.com/nixys/nxs-go-appctx/v2"

	"nxs-backup/modules/storage"
)

type Storage interface {
	IsLocal() int
	SetBackupPath(path string)
	SetRetention(r storage.Retention)
	DeliveryDescBackup(appCtx *appctx.AppContext, tmpBackupPath, ofs string) error
	DeliveryIncBackup(appCtx *appctx.AppContext, tmpBackupPath, ofs string, init bool) error
	DeliveryIncBackupMetadata(appCtx *appctx.AppContext, tmpBackupMetadata, ofs string, init bool) error
	DeleteOldDescBackups(appCtx *appctx.AppContext, ofsPartsList []string) error
	GetFile(path string) (fs.File, error)
	Close() error
	Clone() Storage
}

type Storages []Storage

func (s Storages) Len() int           { return len(s) }
func (s Storages) Less(i, j int) bool { return s[i].IsLocal() < s[j].IsLocal() }
func (s Storages) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func (s Storages) DeleteOldBackups(appCtx *appctx.AppContext, j Job) (errs []error) {
	ofsPartsList := j.GetTargetOfsList()
	if j.GetType() == "inc_files" {
		// TODO delete old inc backups
	} else {
		for _, st := range s {
			err := st.DeleteOldDescBackups(appCtx, ofsPartsList)
			if err != nil {
				errs = append(errs, err)
			}
		}
	}

	return
}

func (s Storages) Delivery(appCtx *appctx.AppContext, job Job) (errs []error) {

	incremental := job.GetType() == "inc_files"

	for ofs, tmpBackupFile := range job.GetDumpedObjects() {

		if incremental {
			tmpMeta := path.Join(path.Dir(tmpBackupFile), path.Base(tmpBackupFile)+".inc")
			initFile := path.Join(path.Dir(tmpBackupFile), path.Base(tmpBackupFile)+"init")

			init := true
			if _, err := os.Stat(initFile); errors.Is(err, fs.ErrNotExist) {
				init = false
			}

			for _, st := range s {
				if err := st.DeliveryIncBackup(appCtx, tmpBackupFile, ofs, init); err != nil {
					errs = append(errs, err)
				}
				if err := st.DeliveryIncBackupMetadata(appCtx, tmpMeta, ofs, init); err != nil {
					errs = append(errs, err)
				}
			}
			// cleanup tmp metadata
			_ = os.Remove(tmpMeta)
			if init {
				_ = os.Remove(initFile)
			}
		} else {
			for _, st := range s {
				if err := st.DeliveryDescBackup(appCtx, tmpBackupFile, ofs); err != nil {
					errs = append(errs, err)
				}
			}

			if err := os.Remove(tmpBackupFile); err != nil {
				errs = append(errs, err)
			}
			appCtx.Log().Infof("deleted temp backup file '%s'", tmpBackupFile)
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
