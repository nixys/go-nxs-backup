package ftp

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jlaffaye/ftp"
	appctx "github.com/nixys/nxs-go-appctx/v2"

	"nxs-backup/interfaces"
	"nxs-backup/misc"
	. "nxs-backup/modules/storage"
)

type FTP struct {
	conn    *ftp.ServerConn
	bakPath string
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

	c, err := ftp.Dial(fmt.Sprintf("%s:%d", params.Host, params.Port),
		ftp.DialWithTimeout(params.ConnectionTimeout*time.Second))
	if err != nil {
		return
	}

	err = c.Login(params.User, params.Password)
	if err != nil {
		return
	}

	err = c.NoOp()
	if err != nil {
		_ = c.Quit()
		return
	}

	s = &FTP{
		conn: c,
	}

	return
}

func (f *FTP) IsLocal() int { return 0 }

func (f *FTP) SetBackupPath(path string) {
	f.bakPath = path
}

func (f *FTP) SetRetention(r Retention) {
	f.Retention = r
}

func (f *FTP) DeliveryBackup(appCtx *appctx.AppContext, tmpBackupFile, ofs string, bakType string) error {
	var bakRemPaths, mtdRemPaths []string

	if bakType == misc.IncBackupType {
		bakRemPaths, mtdRemPaths = GetIncBackupDstList(path.Base(tmpBackupFile), ofs, f.bakPath)
		if err := f.deliveryBackupMetadata(appCtx, tmpBackupFile, mtdRemPaths); err != nil {
			return err
		}
	} else {
		bakRemPaths = GetDescBackupDstList(path.Base(tmpBackupFile), ofs, f.bakPath, f.Retention)
	}

	for _, dstPath := range bakRemPaths {
		if err := f.copy(appCtx, dstPath, tmpBackupFile); err != nil {
			return err
		}
	}

	return nil
}

func (f *FTP) deliveryBackupMetadata(appCtx *appctx.AppContext, tmpBackupFile string, mtdDstPaths []string) error {
	mtdSrcPath := tmpBackupFile + ".inc"

	for _, dstPath := range mtdDstPaths {
		if err := f.copy(appCtx, dstPath, mtdSrcPath); err != nil {
			return err
		}
	}

	return nil
}

func (f *FTP) copy(appCtx *appctx.AppContext, dst, src string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		appCtx.Log().Errorf("Unable to open file: '%s'", err)
		return err
	}
	defer func() { _ = srcFile.Close() }()

	// Make remote directories
	dstDir := path.Dir(dst)
	err = f.mkDir(dstDir)
	if err != nil {
		appCtx.Log().Errorf("Unable to create remote directory '%s': '%s'", dstDir, err)
		return err
	}

	err = f.conn.Stor(dst, srcFile)
	if err != nil {
		appCtx.Log().Errorf("Unable to upload file: %s", err)
		return err
	}
	appCtx.Log().Infof("Successfully uploaded file '%s'", dst)
	return nil
}

func (f *FTP) DeleteOldBackups(appCtx *appctx.AppContext, ofsPartsList []string, bakType string) error {

	var errs []error
	curDate := time.Now()
	// TODO delete old inc backups
	for _, ofsPart := range ofsPartsList {

		if bakType == misc.IncBackupType {
			intMoy, _ := strconv.Atoi(misc.GetDateTimeNow("moy"))
			lastMonth := intMoy - f.Months

			var year string
			if lastMonth > 0 {
				year = misc.GetDateTimeNow("year")
			} else {
				year = misc.GetDateTimeNow("previous_year")
				lastMonth += 12
			}

			backupDir := path.Join(f.bakPath, ofsPart, year)

			dirs, err := f.conn.List(backupDir)
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					return nil
				} else {
					appCtx.Log().Errorf("Failed to get access to directory '%s' with next error: %v", backupDir, err)
					return err
				}
			}

			for _, dir := range dirs {
				rx := regexp.MustCompile("month_\\d\\d")
				if rx.MatchString(dir.Name) {
					dirParts := strings.Split(dir.Name, "_")
					dirMonth, _ := strconv.Atoi(dirParts[1])
					if dirMonth < lastMonth {
						err = f.conn.RemoveDirRecur(path.Join(backupDir, dir.Name))
						if err != nil {
							appCtx.Log().Errorf("Failed to delete '%s' in dir '%s' with next error: %s",
								dir.Name, backupDir, err)
							errs = append(errs, err)
						}
					}
				}
			}
		} else {
			for _, period := range []string{"daily", "weekly", "monthly"} {
				bakDir := path.Join(f.bakPath, ofsPart, period)
				files, err := f.conn.List(bakDir)
				if err != nil {
					if os.IsNotExist(err) {
						continue
					}
					appCtx.Log().Errorf("Failed to read files in remote directory '%s' with next error: %s", bakDir, err)
					return err
				}

				for _, file := range files {

					fileDate := file.Time
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
						err = f.conn.Delete(path.Join(bakDir, file.Name))
						if err != nil {
							appCtx.Log().Errorf("Failed to delete file '%s' in remote directory '%s' with next error: %s",
								file.Name, bakDir, err)
							errs = append(errs, err)
						} else {
							appCtx.Log().Infof("Deleted old backup file '%s' in remote directory '%s'", file.Name, bakDir)
						}
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

func (f *FTP) mkDir(dstPath string) error {

	dstPath = path.Clean(dstPath)
	if dstPath == "." || dstPath == "/" {
		return nil
	}

	err := f.conn.MakeDir(dstPath)
	if err != nil {
		return err
	}

	return nil
}

func (f *FTP) GetFile(ofsPath string) (fs.File, error) {
	//TODO implement me
	panic("implement me")
}

func (f *FTP) Close() error {
	return f.conn.Quit()
}

func (f *FTP) Clone() interfaces.Storage {
	cl := *f
	return &cl
}
