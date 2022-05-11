package storage

import (
	appctx "github.com/nixys/nxs-go-appctx/v2"
	"github.com/pkg/sftp"
)

type SFTP struct {
	Client     *sftp.Client
	BackupPath string
	Retention
}

func (s *SFTP) IsLocal() int { return 0 }

func (s *SFTP) ListFiles() (err error) {
	return
}

func (s *SFTP) CopyFile(appCtx *appctx.AppContext, tmpBackup, ofs string, _ bool) error {
	return nil
}

func (s *SFTP) ControlFiles(appCtx *appctx.AppContext, ofsPartsList []string) (errs []error) {
	return
}
