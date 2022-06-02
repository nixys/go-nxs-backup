package storage

import (
	"errors"
	"fmt"
	"os"
	"path"
	"time"

	appctx "github.com/nixys/nxs-go-appctx/v2"

	"nxs-backup/misc"
	"nxs-backup/modules/backend/webdav"
)

type WebDav struct {
	Client     *webdav.Client
	BackupPath string
	Retention
}

func (s *WebDav) IsLocal() int { return 0 }

func (s *WebDav) BackupPathSet(path string) {
	s.BackupPath = path
}

func (s *WebDav) RetentionSet(r Retention) {
	s.Retention = r
}

func (s *WebDav) ListFiles() (err error) {
	return
}

func (s *WebDav) CopyFile(appCtx *appctx.AppContext, tmpBackup, ofs string, _ bool) error {
	srcFile, err := os.Open(tmpBackup)
	if err != nil {
		appCtx.Log().Errorf("Unable to open tmp backup: '%s'", err)
		return err
	}
	defer srcFile.Close()

	dstPath, links, err := misc.GetDstAndLinks(path.Base(tmpBackup), ofs, s.BackupPath, s.Days, s.Weeks, s.Months)
	if err != nil {
		appCtx.Log().Errorf("Unable to get destination path and links: '%s'", err)
		return err
	}

	// Make remote directories
	remDir := path.Dir(dstPath)
	err = s.mkDir(remDir)
	if err != nil {
		appCtx.Log().Errorf("Unable to create remote directory '%s': '%s'", remDir, err)
		return err
	}

	err = s.Client.Upload(dstPath, srcFile)
	if err != nil {
		appCtx.Log().Errorf("Unable to upload file: %s", err)
		return err
	}
	appCtx.Log().Infof("%s crated", dstPath)

	for dst, src := range links {
		remDir = path.Dir(dst)
		err = s.mkDir(path.Dir(dst))
		if err != nil {
			appCtx.Log().Errorf("Unable to create remote directory '%s': '%s'", remDir, err)
			return err
		}
		err = s.Client.Copy(src, dst)
		if err != nil {
			appCtx.Log().Errorf("Unable to make copy: %s", err)
			return err
		}
	}

	return nil
}

func (s *WebDav) ControlFiles(appCtx *appctx.AppContext, ofsPartsList []string) (errs []error) {

	curDate := time.Now()

	for _, period := range []string{"daily", "weekly", "monthly"} {
		for _, ofsPart := range ofsPartsList {
			bakDir := path.Join(s.BackupPath, ofsPart, period)
			files, err := s.Client.Ls(bakDir)
			if err != nil {
				if os.IsNotExist(err) {
					appCtx.Log().Warnf("Error: '%s' %s", bakDir, err)
					continue
				}
				appCtx.Log().Errorf("Failed to read files in remote directory '%s' with next error: %s", bakDir, err)
				return []error{err}
			}

			for _, file := range files {

				fileDate := file.ModTime()
				var retentionDate time.Time

				switch period {
				case "daily":
					retentionDate = fileDate.AddDate(0, 0, s.Retention.Days)
				case "weekly":
					retentionDate = fileDate.AddDate(0, 0, s.Retention.Weeks*7)
				case "monthly":
					retentionDate = fileDate.AddDate(0, s.Retention.Months, 0)
				}

				retentionDate = retentionDate.Truncate(24 * time.Hour)
				if curDate.After(retentionDate) {
					err = s.Client.Rm(path.Join(bakDir, file.Name()))
					if err != nil {
						appCtx.Log().Errorf("Failed to delete file '%s' in remote directory '%s' with next error: %s",
							file.Name(), bakDir, err)
						errs = append(errs, err)
					} else {
						appCtx.Log().Infof("Deleted old backup file '%s' in remote directory '%s'", file.Name(), bakDir)
					}
				}
			}
		}
	}
	return
}

func (s *WebDav) mkDir(dstPath string) error {

	dstPath = path.Clean(dstPath)
	if dstPath == "." || dstPath == "/" {
		return nil
	}
	fi, err := s.getInfo(dstPath)
	if err == nil {
		if fi.IsDir() {
			return nil
		}
		return errors.New(fmt.Sprintf("%s is a file not a directory", dstPath))
	} else if err != ErrorFileNotFound {
		return fmt.Errorf("mkdir %q failed: %w", dstPath, err)
	}

	dir := path.Dir(dstPath)
	err = s.mkDir(dir)
	if err != nil {
		return err
	}
	err = s.Client.Mkdir(dstPath)
	if err != nil {
		return err
	}

	return nil
}

func (s *WebDav) getInfo(dstPath string) (os.FileInfo, error) {

	dir := path.Dir(dstPath)
	base := path.Base(dstPath)

	files, err := s.Client.Ls(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrorFileNotFound
		}
		return nil, err
	}

	for _, file := range files {
		if file.Name() == base {
			return file, nil
		}
	}
	return nil, ErrorFileNotFound
}
