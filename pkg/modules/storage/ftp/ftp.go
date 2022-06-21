package ftp

import (
	"crypto/tls"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	appctx "github.com/nixys/nxs-go-appctx/v2"
	"github.com/prasad83/goftp"

	"nxs-backup/misc"
	. "nxs-backup/modules/storage"
)

type FTP struct {
	Client     *goftp.Client
	BackupPath string
	Retention
}

type Params struct {
	Host              string
	User              string
	Password          string
	Port              int
	ConnectCount      int
	ConnectionTimeout time.Duration
}

func Init(params Params) (s *FTP, err error) {

	configWithoutTLS := goftp.Config{
		User:               params.User,
		Password:           params.Password,
		ConnectionsPerHost: params.ConnectCount,
		Timeout:            params.ConnectionTimeout * time.Minute,
		//Logger:             os.Stdout,
	}
	configWithTLS := configWithoutTLS
	configWithTLS.TLSConfig = &tls.Config{
		InsecureSkipVerify: true,
		//ClientSessionCache: tls.NewLRUClientSessionCache(32),
	}
	//configWithTLS.TLSMode = goftp.TLSExplicit

	var client *goftp.Client
	// Attempt to connect using FTPS
	if client, err = goftp.DialConfig(configWithTLS, fmt.Sprintf("%s:%d", strings.TrimPrefix(params.Host, "ftps://"), params.Port)); err == nil {
		if _, err = client.ReadDir("/"); err != nil {
			_ = client.Close()
		} else {
			s = &FTP{
				Client: client,
			}
		}
	}

	// Attempt to create an FTP connection if FTPS isn't available
	if s == nil {
		client, err = goftp.DialConfig(configWithoutTLS, fmt.Sprintf("%s:%d", strings.TrimPrefix(params.Host, "ftp://"), params.Port))
		if err != nil {
			return
		}
		if _, err = client.ReadDir("/"); err != nil {
			_ = client.Close()
			return
		}
		s = &FTP{
			Client: client,
		}
	}

	return
}

func (f *FTP) IsLocal() int { return 0 }

func (f *FTP) SetBackupPath(path string) {
	f.BackupPath = path
}

func (f *FTP) SetRetention(r Retention) {
	f.Retention = r
}

func (f *FTP) CopyFile(appCtx *appctx.AppContext, tmpBackup, ofs string, _ bool) error {

	srcFile, err := os.Open(tmpBackup)
	if err != nil {
		appCtx.Log().Errorf("Unable to open tmp backup: '%s'", err)
		return err
	}
	defer srcFile.Close()

	//var buf []byte
	//_, err = srcFile.Read(buf)
	//if err != nil {
	//	appCtx.Log().Errorf("Unable to open tmp backup: '%f'", err)
	//	return err
	//}

	remotePaths := misc.GetDstList(path.Base(tmpBackup), ofs, f.BackupPath, f.Days, f.Weeks, f.Months)

	for _, dstPath := range remotePaths {
		// Make remote directories
		dstDir := path.Dir(dstPath)
		err = f.mkDir(dstDir)
		if err != nil {
			appCtx.Log().Errorf("Unable to create remote directory '%s': '%s'", dstDir, err)
			return err
		}

		err = f.Client.Store(dstPath, srcFile)
		//// Ignore error 250 here - send by some servers
		//if err != nil {
		//	switch errX := err.(type) {
		//	case *textproto.Error:
		//		switch errX.Code {
		//		case ftp.StatusRequestedFileActionOK:
		//			err = nil
		//		}
		//	}
		//}
		if err != nil {
			//_ = f.Client.Close()
			appCtx.Log().Errorf("Unable to upload file: %s", err)
			return err
		}
		appCtx.Log().Infof("%s file successfully uploaded", srcFile.Name())

	}

	return nil
}

func (f *FTP) ControlFiles(appCtx *appctx.AppContext, ofsPartsList []string) (errs []error) {

	curDate := time.Now()

	for _, period := range []string{"daily", "weekly", "monthly"} {
		for _, ofsPart := range ofsPartsList {
			bakDir := path.Join(f.BackupPath, ofsPart, period)
			files, err := f.Client.ReadDir(bakDir)
			if err != nil {
				if os.IsNotExist(err) {
					appCtx.Log().Warnf("Error: %s", err)
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
					retentionDate = fileDate.AddDate(0, 0, f.Retention.Days)
				case "weekly":
					retentionDate = fileDate.AddDate(0, 0, f.Retention.Weeks*7)
				case "monthly":
					retentionDate = fileDate.AddDate(0, f.Retention.Months, 0)
				}

				retentionDate = retentionDate.Truncate(24 * time.Hour)
				if curDate.After(retentionDate) {
					err = f.Client.Delete(path.Join(bakDir, file.Name()))
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

func (f FTP) mkDir(dstPath string) error {

	dstPath = path.Clean(dstPath)
	if dstPath == "." || dstPath == "/" {
		return nil
	}
	fi, err := f.getInfo(dstPath)
	if err == nil {
		if fi.IsDir() {
			return nil
		}
		return errors.New(fmt.Sprintf("%s is a file not a directory", dstPath))
	} else if err != ErrorObjectNotFound {
		return fmt.Errorf("mkdir %q failed: %w", dstPath, err)
	}

	dir := path.Dir(dstPath)
	err = f.mkDir(dir)
	if err != nil {
		return err
	}
	_, err = f.Client.Mkdir(dstPath)
	if err != nil {
		return err
	}

	return nil
}

func (f FTP) getInfo(dstPath string) (os.FileInfo, error) {

	dir := path.Dir(dstPath)
	base := path.Base(dstPath)

	files, err := f.Client.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if file.Name() == base {
			return file, nil
		}
	}
	return nil, ErrorObjectNotFound
}
