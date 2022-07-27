package webdav

import (
	"errors"
	"fmt"
	"os"
	"path"
	"time"

	appctx "github.com/nixys/nxs-go-appctx/v2"

	"nxs-backup/interfaces"
	"nxs-backup/modules/backend/webdav"
	. "nxs-backup/modules/storage"
)

type WebDav struct {
	Client     *webdav.Client
	BackupPath string
	Retention
}

type Params struct {
	URL               string
	Username          string
	Password          string
	OAuthToken        string
	ConnectionTimeout time.Duration
}

func Init(params Params) (*WebDav, error) {

	client, err := webdav.Init(webdav.Params{
		URL:               params.URL,
		Username:          params.Username,
		Password:          params.Password,
		OAuthToken:        params.OAuthToken,
		ConnectionTimeout: params.ConnectionTimeout,
	})
	if err != nil {
		return nil, err
	}

	return &WebDav{
		Client: client,
	}, nil
}

func (wd *WebDav) IsLocal() int { return 0 }

func (wd *WebDav) SetBackupPath(path string) {
	wd.BackupPath = path
}

func (wd *WebDav) SetRetention(r Retention) {
	wd.Retention = r
}

func (wd *WebDav) ListFiles() (err error) {
	return
}

func (wd *WebDav) CopyFile(appCtx *appctx.AppContext, tmpBackup, ofs string, _ bool) error {
	srcFile, err := os.Open(tmpBackup)
	if err != nil {
		appCtx.Log().Errorf("Unable to open tmp backup: '%s'", err)
		return err
	}
	defer srcFile.Close()

	dstPath, links, err := GetDstAndLinks(path.Base(tmpBackup), ofs, wd.BackupPath, wd.Days, wd.Weeks, wd.Months)
	if err != nil {
		appCtx.Log().Errorf("Unable to get destination path and links: '%s'", err)
		return err
	}

	// Make remote directories
	remDir := path.Dir(dstPath)
	err = wd.mkDir(remDir)
	if err != nil {
		appCtx.Log().Errorf("Unable to create remote directory '%s': '%s'", remDir, err)
		return err
	}

	err = wd.Client.Upload(dstPath, srcFile)
	if err != nil {
		appCtx.Log().Errorf("Unable to upload file: %s", err)
		return err
	}
	appCtx.Log().Infof("%s crated", dstPath)

	for dst, src := range links {
		remDir = path.Dir(dst)
		err = wd.mkDir(path.Dir(dst))
		if err != nil {
			appCtx.Log().Errorf("Unable to create remote directory '%s': '%s'", remDir, err)
			return err
		}
		err = wd.Client.Copy(src, dst)
		if err != nil {
			appCtx.Log().Errorf("Unable to make copy: %s", err)
			return err
		}
	}

	return nil
}

func (wd *WebDav) ControlFiles(appCtx *appctx.AppContext, ofsPartsList []string) error {

	var errs []error
	curDate := time.Now()

	for _, period := range []string{"daily", "weekly", "monthly"} {
		for _, ofsPart := range ofsPartsList {
			bakDir := path.Join(wd.BackupPath, ofsPart, period)
			files, err := wd.Client.Ls(bakDir)
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				appCtx.Log().Errorf("Failed to read files in remote directory '%s' with next error: %s", bakDir, err)
				return err
			}

			for _, file := range files {

				fileDate := file.ModTime()
				var retentionDate time.Time

				switch period {
				case "daily":
					retentionDate = fileDate.AddDate(0, 0, wd.Retention.Days)
				case "weekly":
					retentionDate = fileDate.AddDate(0, 0, wd.Retention.Weeks*7)
				case "monthly":
					retentionDate = fileDate.AddDate(0, wd.Retention.Months, 0)
				}

				retentionDate = retentionDate.Truncate(24 * time.Hour)
				if curDate.After(retentionDate) {
					err = wd.Client.Rm(path.Join(bakDir, file.Name()))
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

	if len(errs) > 0 {
		return fmt.Errorf("some errors on file deletion")
	}

	return nil
}

func (wd *WebDav) mkDir(dstPath string) error {

	dstPath = path.Clean(dstPath)
	if dstPath == "." || dstPath == "/" {
		return nil
	}
	fi, err := wd.getInfo(dstPath)
	if err == nil {
		if fi.IsDir() {
			return nil
		}
		return errors.New(fmt.Sprintf("%s is a file not a directory", dstPath))
	} else if err != ErrorFileNotFound {
		return fmt.Errorf("mkdir %q failed: %w", dstPath, err)
	}

	dir := path.Dir(dstPath)
	err = wd.mkDir(dir)
	if err != nil {
		return err
	}
	err = wd.Client.Mkdir(dstPath)
	if err != nil {
		return err
	}

	return nil
}

func (wd *WebDav) getInfo(dstPath string) (os.FileInfo, error) {

	dir := path.Dir(dstPath)
	base := path.Base(dstPath)

	files, err := wd.Client.Ls(dir)
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

func (wd *WebDav) Close() error {
	return nil
}

func (wd *WebDav) Clone() interfaces.Storage {
	cl := *wd
	return &cl
}
