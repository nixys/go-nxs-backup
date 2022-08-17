package nfs

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"time"

	appctx "github.com/nixys/nxs-go-appctx/v2"
	"github.com/vmware/go-nfs-client/nfs"
	"github.com/vmware/go-nfs-client/nfs/rpc"

	"nxs-backup/interfaces"
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

func (n *NFS) IsLocal() int { return 0 }

func (n *NFS) SetBackupPath(path string) {
	n.BackupPath = path
}

func (n *NFS) SetRetention(r Retention) {
	n.Retention = r
}

func (n *NFS) DeliveryBackup(appCtx *appctx.AppContext, tmpBackup, ofs, bakType string) error {

	source, err := os.Open(tmpBackup)
	if err != nil {
		return err
	}
	defer source.Close()

	remotePaths := GetDescBackupDstList(path.Base(tmpBackup), ofs, n.BackupPath, n.Retention)

	for _, dstPath := range remotePaths {
		// Make remote directories
		dstDir := path.Dir(dstPath)
		err = n.mkDir(dstDir)
		if err != nil {
			appCtx.Log().Errorf("Unable to create remote directory '%s': '%s'", dstDir, err)
			return err
		}

		destination, err := n.Target.OpenFile(dstPath, 0666)
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
		appCtx.Log().Infof("Successfully copied temp backup to %s", dstPath)
	}

	return nil
}

func (n *NFS) DeleteOldBackups(appCtx *appctx.AppContext, ofsPartsList []string, bakType string) error {

	var errs []error
	curDate := time.Now()
	// TODO delete old inc backups

	for _, period := range []string{"daily", "weekly", "monthly"} {
		for _, ofsPart := range ofsPartsList {
			bakDir := path.Join(n.BackupPath, ofsPart, period)
			files, err := n.Target.ReadDirPlus(bakDir)
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
					retentionDate = fileDate.AddDate(0, 0, n.Retention.Days)
				case "weekly":
					retentionDate = fileDate.AddDate(0, 0, n.Retention.Weeks*7)
				case "monthly":
					retentionDate = fileDate.AddDate(0, n.Retention.Months, 0)
				}

				retentionDate = retentionDate.Truncate(24 * time.Hour)
				if curDate.After(retentionDate) {
					err = n.Target.Remove(path.Join(bakDir, file.Name()))
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

func (n *NFS) mkDir(dstPath string) error {

	dstPath = path.Clean(dstPath)
	if dstPath == "." || dstPath == "/" {
		return nil
	}
	fi, err := n.getInfo(dstPath)
	if err == nil {
		if fi.IsDir() {
			return nil
		}
		return errors.New(fmt.Sprintf("%s is a file not a directory", dstPath))
	} else if err != ErrorFileNotFound {
		return fmt.Errorf("mkdir %q failed: %w", dstPath, err)
	}

	dir := path.Dir(dstPath)
	err = n.mkDir(dir)
	if err != nil {
		return err
	}
	_, err = n.Target.Mkdir(dstPath, os.ModePerm)
	if err != nil {
		return err
	}

	return nil
}

func (n *NFS) getInfo(dstPath string) (os.FileInfo, error) {

	dir := path.Dir(dstPath)
	base := path.Base(dstPath)

	files, err := n.Target.ReadDirPlus(dir)
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

func (n *NFS) GetFile(ofsPath string) (fs.File, error) {
	//TODO implement me
	panic("implement me")
}

func (n *NFS) Close() error {
	return n.Target.Close()
}

func (n *NFS) Clone() interfaces.Storage {
	cl := *n
	return &cl
}
