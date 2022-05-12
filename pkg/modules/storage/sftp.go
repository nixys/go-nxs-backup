package storage

import (
	appctx "github.com/nixys/nxs-go-appctx/v2"
	"github.com/pkg/sftp"
	"io"
	"nxs-backup/misc"
	"os"
	"path/filepath"
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

	srcFile, err := os.Open(tmpBackup)
	if err != nil {
		appCtx.Log().Errorf("Unable to open tmp backup: '%s'", err)
		return err
	}
	defer srcFile.Close()

	dstPath, links, err := misc.GetDstAndLinks(filepath.Base(tmpBackup), ofs, s.BackupPath, s.Days, s.Weeks, s.Months)
	if err != nil {
		appCtx.Log().Errorf("Unable to get destination path and links: '%s'", err)
		return err
	}

	// Make remote directories
	rmDir := filepath.Dir(dstPath)
	err = s.Client.MkdirAll(rmDir)
	if err != nil {
		appCtx.Log().Errorf("Unable to create remote directory '%s': '%s'", rmDir, err)
		return err
	}

	dstFile, err := s.Client.Create(dstPath)
	if err != nil {
		appCtx.Log().Errorf("Unable to create remote file: %v", err)
		return err
	}
	defer dstFile.Close()

	bytes, err := io.Copy(dstFile, srcFile)
	if err != nil {
		appCtx.Log().Errorf("Unable to upload file: %v", err)
		return err
	}
	appCtx.Log().Infof("%s file crated. %d bytes copied", dstFile.Name(), bytes)

	for dst, src := range links {
		rmDir = filepath.Dir(dst)
		err = s.Client.MkdirAll(filepath.Dir(dst))
		if err != nil {
			appCtx.Log().Errorf("Unable to create remote directory '%s': '%s'", rmDir, err)
			return err
		}
		err = s.Client.Symlink(src, dst)
		if err != nil {
			appCtx.Log().Errorf("Unable to create symlink: %v", err)
			return err
		}
	}

	return nil
}

func (s *SFTP) ControlFiles(appCtx *appctx.AppContext, ofsPartsList []string) (errs []error) {
	return
}
