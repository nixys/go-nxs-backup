package interfaces

import (
	"io"
	"os"
	"path"

	"github.com/hashicorp/go-multierror"
	appctx "github.com/nixys/nxs-go-appctx/v2"

	"nxs-backup/misc"
	"nxs-backup/modules/storage"
)

type Storage interface {
	IsLocal() int
	SetBackupPath(path string)
	SetRetention(r storage.Retention)
	DeliveryBackup(appCtx *appctx.AppContext, tmpBackupPath, ofs, bakType string) error
	DeleteOldBackups(appCtx *appctx.AppContext, ofsPartsList []string, bakType string, full bool) error
	GetFileReader(path string) (io.Reader, error)
	Close() error
	Clone() Storage
}

type Storages []Storage

func (s Storages) Len() int           { return len(s) }
func (s Storages) Less(i, j int) bool { return s[i].IsLocal() < s[j].IsLocal() }
func (s Storages) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func (s Storages) DeleteOldBackups(appCtx *appctx.AppContext, j Job, ofsPath string) (errs []error) {
	var err error
	for _, st := range s {
		if ofsPath != "" {
			err = st.DeleteOldBackups(appCtx, []string{ofsPath}, j.GetType(), true)
		} else {
			err = st.DeleteOldBackups(appCtx, j.GetTargetOfsList(), j.GetType(), false)
		}
		if err != nil {
			errs = append(errs, err)
		}
	}
	return
}

func (s Storages) Delivery(appCtx *appctx.AppContext, job Job) error {

	var errs *multierror.Error

	for ofs, dumpObj := range job.GetDumpObjects() {
		if !dumpObj.Delivered {
			var errsDelivery []error
			for _, st := range s {
				if err := st.DeliveryBackup(appCtx, dumpObj.TmpFile, ofs, job.GetType()); err != nil {
					errsDelivery = append(errsDelivery, err)
				}
			}
			if len(errsDelivery) == 0 {
				job.SetDumpObjectDelivered(ofs)
			} else {
				errs = multierror.Append(errs, errsDelivery...)
			}
		}
	}

	return errs.ErrorOrNil()
}

func (s Storages) CleanupTmpData(appCtx *appctx.AppContext, job Job) error {
	var errs *multierror.Error

	for _, dumpObj := range job.GetDumpObjects() {

		tmpBakFile := dumpObj.TmpFile
		if job.GetType() == misc.IncBackupType {
			// cleanup tmp metadata files
			_ = os.Remove(path.Join(tmpBakFile + ".inc"))
			initFile := path.Join(tmpBakFile + ".init")
			if _, err := os.Stat(initFile); err == nil {
				_ = os.Remove(initFile)
			}
		}

		// cleanup tmp backup file
		if err := os.Remove(tmpBakFile); err != nil {
			errs = multierror.Append(errs, err)
		}
		appCtx.Log().Infof("deleted temp backup file '%s'", tmpBakFile)
	}
	return errs.ErrorOrNil()
}

func (s Storages) Close() error {
	for _, st := range s {
		_ = st.Close()
	}
	return nil
}
