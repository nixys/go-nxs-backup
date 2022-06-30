package nfs

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"time"

	appctx "github.com/nixys/nxs-go-appctx/v2"
	"github.com/vmware/go-nfs-client/nfs"
	"github.com/vmware/go-nfs-client/nfs/rpc"

	"nxs-backup/misc"
	. "nxs-backup/modules/storage"
)

type NFS struct {
	Target     *nfs.Target
	BackupPath string
	Retention
}

type Params struct {
	Host   string
	Target string
	UID    uint32
	GID    uint32
	Port   int
}

func Init(params Params) (*NFS, error) {

	mount, err := nfs.DialMount(params.Host)
	if err != nil {
		return nil, fmt.Errorf("unable to dial MOUNT service: %s", err)
	}

	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	auth := rpc.NewAuthUnix(hostname, params.UID, params.GID)

	target, err := mount.Mount(params.Target, auth.Auth())
	if err != nil {
		return nil, fmt.Errorf("unable to mount volume: %s", err)
	}

	_, err = target.FSInfo()
	if err != nil {
		return nil, fmt.Errorf("unable to get target status: %s", err)
	}

	return &NFS{
		Target: target,
	}, nil
}

func (s *NFS) IsLocal() int { return 0 }

func (s *NFS) SetBackupPath(path string) {
	s.BackupPath = path
}

func (s *NFS) SetRetention(r Retention) {
	s.Retention = r
}

func (s *NFS) CopyFile(appCtx *appctx.AppContext, tmpBackup, ofs string, _ bool) error {

	source, err := os.Open(tmpBackup)
	if err != nil {
		return err
	}
	defer source.Close()

	remotePaths := misc.GetDstList(path.Base(tmpBackup), ofs, s.BackupPath, s.Days, s.Weeks, s.Months)

	for _, dstPath := range remotePaths {
		// Make remote directories
		dstDir := path.Dir(dstPath)
		err = s.mkDir(dstDir)
		if err != nil {
			appCtx.Log().Errorf("Unable to create remote directory '%s': '%s'", dstDir, err)
			return err
		}

		destination, err := s.Target.OpenFile(dstPath, 0666)
		if err != nil {
			appCtx.Log().Errorf("Unable to create destination file '%s': '%s'", dstDir, err)
			return err
		}
		defer destination.Close()

		_, err = io.Copy(destination, source)
		if err != nil {
			appCtx.Log().Errorf("Unable to make copy '%s': '%s'", dstDir, err)
			return err
		}
		appCtx.Log().Infof("Successfully copied file '%s' to %s", source.Name(), dstPath)
	}

	return nil
}

func (s *NFS) ControlFiles(appCtx *appctx.AppContext, ofsPartsList []string) error {

	var errs []error
	curDate := time.Now()

	for _, period := range []string{"daily", "weekly", "monthly"} {
		for _, ofsPart := range ofsPartsList {
			bakDir := path.Join(s.BackupPath, ofsPart, period)
			files, err := s.Target.ReadDirPlus(bakDir)
			if err != nil {
				if os.IsNotExist(err) {
					appCtx.Log().Warnf("Error: '%s' %s", bakDir, err)
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
					retentionDate = fileDate.AddDate(0, 0, s.Retention.Days)
				case "weekly":
					retentionDate = fileDate.AddDate(0, 0, s.Retention.Weeks*7)
				case "monthly":
					retentionDate = fileDate.AddDate(0, s.Retention.Months, 0)
				}

				retentionDate = retentionDate.Truncate(24 * time.Hour)
				if curDate.After(retentionDate) {
					err = s.Target.Remove(path.Join(bakDir, file.Name()))
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

func (s *NFS) mkDir(dstPath string) error {

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
	_, err = s.Target.Mkdir(dstPath, os.ModePerm)
	if err != nil {
		return err
	}

	return nil
}

func (s *NFS) getInfo(dstPath string) (os.FileInfo, error) {

	dir := path.Dir(dstPath)
	base := path.Base(dstPath)

	files, err := s.Target.ReadDirPlus(dir)
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
