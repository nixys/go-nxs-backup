package nfs

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
	appctx "github.com/nixys/nxs-go-appctx/v2"
	"github.com/sirupsen/logrus"
	"github.com/vmware/go-nfs-client/nfs"
	"github.com/vmware/go-nfs-client/nfs/rpc"

	"nxs-backup/interfaces"
	"nxs-backup/misc"
	. "nxs-backup/modules/storage"
)

type NFS struct {
	target     *nfs.Target
	backupPath string
	name       string
	logFields  logrus.Fields
	Retention
}

type Params struct {
	Host   string
	Target string
	UID    uint32
	GID    uint32
	Port   int
}

func Init(name string, params Params) (*NFS, error) {
	mount, err := nfs.DialMount(params.Host)
	if err != nil {
		return nil, fmt.Errorf("Failed to init '%s' NFS storage. Dial MOUNT service error: %v ", name, err)
	}

	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	auth := rpc.NewAuthUnix(hostname, params.UID, params.GID)

	target, err := mount.Mount(params.Target, auth.Auth())
	if err != nil {
		return nil, fmt.Errorf("Failed to init '%s' NFS storage. Mount volume error: %v ", name, err)
	}

	_, err = target.FSInfo()
	if err != nil {
		return nil, fmt.Errorf("Failed to init '%s' NFS storage. Get target status error: %v ", name, err)
	}

	return &NFS{
		name:      name,
		logFields: logrus.Fields{"storage": name},
		target:    target,
	}, nil
}

func (n *NFS) IsLocal() int { return 0 }

func (n *NFS) SetBackupPath(path string) {
	n.backupPath = path
}

func (n *NFS) SetRetention(r Retention) {
	n.Retention = r
}

func (n *NFS) DeliveryBackup(appCtx *appctx.AppContext, tmpBackupFile, ofs, bakType string) error {
	var bakRemPaths, mtdRemPaths []string

	if bakType == misc.IncBackupType {
		bakRemPaths, mtdRemPaths = GetIncBackupDstList(tmpBackupFile, ofs, n.backupPath)
	} else {
		bakRemPaths = GetDescBackupDstList(tmpBackupFile, ofs, n.backupPath, n.Retention)
	}

	if len(mtdRemPaths) > 0 {
		for _, dstPath := range mtdRemPaths {
			if err := n.copy(appCtx, dstPath, tmpBackupFile+".inc"); err != nil {
				return err
			}
		}
	}

	for _, dstPath := range bakRemPaths {
		if err := n.copy(appCtx, dstPath, tmpBackupFile); err != nil {
			return err
		}
	}

	return nil
}

func (n *NFS) copy(appCtx *appctx.AppContext, dst, src string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		appCtx.Log().WithFields(n.logFields).Errorf("Unable to open file: '%s'", err)
		return err
	}
	defer func() { _ = srcFile.Close() }()

	// Make remote directories
	dstDir := path.Dir(dst)
	err = n.mkDir(dstDir)
	if err != nil {
		appCtx.Log().WithFields(n.logFields).Errorf("Unable to create remote directory '%s': '%s'", dstDir, err)
		return err
	}

	destination, err := n.target.OpenFile(dst, 0666)
	if err != nil {
		appCtx.Log().WithFields(n.logFields).Errorf("Unable to create destination file '%s': '%s'", dstDir, err)
		return err
	}
	defer func() { _ = destination.Close() }()

	_, err = io.Copy(destination, srcFile)
	if err != nil {
		appCtx.Log().WithFields(n.logFields).Errorf("Unable to make copy '%s': '%s'", dstDir, err)
		return err
	}
	appCtx.Log().WithFields(n.logFields).Infof("Successfully copied temp backup to %s", dst)
	return nil
}

func (n *NFS) DeleteOldBackups(appCtx *appctx.AppContext, ofsPartsList []string, bakType string, full bool) (err error) {

	var errs *multierror.Error

	for _, ofsPart := range ofsPartsList {
		if bakType == misc.IncBackupType {
			err = n.deleteIncBackup(appCtx, ofsPart, full)
		} else {
			err = n.deleteDescBackup(appCtx, ofsPart)
		}
		if err != nil {
			errs = multierror.Append(errs, err)
		}
	}

	return errs.ErrorOrNil()
}

func (n *NFS) deleteDescBackup(appCtx *appctx.AppContext, ofsPart string) error {
	var errs *multierror.Error
	curDate := time.Now()

	for _, period := range []string{"daily", "weekly", "monthly"} {
		bakDir := path.Join(n.backupPath, ofsPart, period)
		files, err := n.target.ReadDirPlus(bakDir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			appCtx.Log().WithFields(n.logFields).Errorf("Failed to read files in remote directory '%s' with next error: %s", bakDir, err)
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
				err = n.target.Remove(path.Join(bakDir, file.Name()))
				if err != nil {
					appCtx.Log().WithFields(n.logFields).Errorf("Failed to delete file '%s' in remote directory '%s' with next error: %s",
						file.Name(), bakDir, err)
					errs = multierror.Append(errs, err)
				} else {
					appCtx.Log().WithFields(n.logFields).Infof("Deleted old backup file '%s' in remote directory '%s'", file.Name(), bakDir)
				}
			}
		}
	}

	return errs
}

func (n *NFS) deleteIncBackup(appCtx *appctx.AppContext, ofsPart string, full bool) error {
	var errs *multierror.Error

	if full {
		backupDir := path.Join(n.backupPath, ofsPart)

		err := n.target.RemoveAll(backupDir)
		if err != nil {
			appCtx.Log().WithFields(n.logFields).Errorf("Failed to delete '%s' with next error: %s", backupDir, err)
			errs = multierror.Append(errs, err)
		}
	} else {
		intMoy, _ := strconv.Atoi(misc.GetDateTimeNow("moy"))
		lastMonth := intMoy - n.Months

		var year string
		if lastMonth > 0 {
			year = misc.GetDateTimeNow("year")
		} else {
			year = misc.GetDateTimeNow("previous_year")
			lastMonth += 12
		}

		backupDir := path.Join(n.backupPath, ofsPart, year)

		dirs, err := n.target.ReadDirPlus(backupDir)
		if err != nil {
			appCtx.Log().WithFields(n.logFields).Errorf("Failed to get access to directory '%s' with next error: %v", backupDir, err)
			return err
		}

		for _, dir := range dirs {
			dirName := dir.Name()
			rx := regexp.MustCompile("month_\\d\\d")
			if rx.MatchString(dirName) {
				dirParts := strings.Split(dirName, "_")
				dirMonth, _ := strconv.Atoi(dirParts[1])
				if dirMonth < lastMonth {
					if err = n.target.RemoveAll(path.Join(backupDir, dirName)); err != nil {
						appCtx.Log().WithFields(n.logFields).Errorf("Failed to delete '%s' in dir '%s' with next error: %s",
							dir.Name, backupDir, err)
						errs = multierror.Append(errs, err)
					}
				}
			}
		}
	}

	return errs.ErrorOrNil()
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
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("mkdir %q failed: %w", dstPath, err)
	}

	dir := path.Dir(dstPath)
	err = n.mkDir(dir)
	if err != nil {
		return err
	}
	_, err = n.target.Mkdir(dstPath, os.ModePerm)
	if err != nil {
		return err
	}

	return nil
}

func (n *NFS) getInfo(dstPath string) (os.FileInfo, error) {

	dir := path.Dir(dstPath)
	base := path.Base(dstPath)

	files, err := n.target.ReadDirPlus(dir)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if file.Name() == base {
			return file, nil
		}
	}
	return nil, fs.ErrNotExist
}

func (n *NFS) GetFileReader(ofsPath string) (io.Reader, error) {

	file, err := n.target.Open(path.Join(n.backupPath, ofsPath))
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	var buf []byte
	buf, err = ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}

	return bytes.NewReader(buf), err
}

func (n *NFS) Close() error {
	return n.target.Close()
}

func (n *NFS) Clone() interfaces.Storage {
	cl := *n
	return &cl
}

func (n *NFS) GetName() string {
	return n.name
}
