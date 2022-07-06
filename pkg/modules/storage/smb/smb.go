package smb

import (
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"strings"
	"time"

	"github.com/hirochachacha/go-smb2"
	appctx "github.com/nixys/nxs-go-appctx/v2"

	"nxs-backup/misc"
	. "nxs-backup/modules/storage"
)

type SMB struct {
	session    *smb2.Session
	share      *smb2.Share
	BackupPath string
	Retention
}

type Params struct {
	Host              string
	Port              int
	User              string
	Password          string
	Domain            string
	Share             string
	ConnectionTimeout time.Duration
}

func Init(params Params) (s *SMB, err error) {
	conn, err := net.DialTimeout(
		"tcp",
		fmt.Sprintf(
			"%s:%d",
			params.Host,
			params.Port,
		),
		params.ConnectionTimeout*time.Second,
	)
	if err != nil {
		return s, err
	}

	s.session, err = (&smb2.Dialer{
		Initiator: &smb2.NTLMInitiator{
			User:     params.User,
			Password: params.Password,
			Domain:   params.Domain,
		},
	}).Dial(conn)
	if err != nil {
		return s, err
	}

	names, err := s.session.ListSharenames()
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		if strings.HasSuffix(name, "$") {
			continue
		}
		if params.Share == name {
			s.share, err = s.session.Mount(name)
			if err != nil {
				return s, err
			}
		}
	}

	return s, nil
}

func (s *SMB) IsLocal() int { return 0 }

func (s *SMB) SetBackupPath(path string) {
	s.BackupPath = strings.TrimPrefix(path, "/")
}

func (s *SMB) SetRetention(r Retention) {
	s.Retention = r
}

func (s *SMB) CopyFile(appCtx *appctx.AppContext, tmpBackup, ofs string, _ bool) error {
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
	err = s.share.MkdirAll(remDir, os.ModeDir)
	if err != nil {
		appCtx.Log().Errorf("Unable to create remote directory '%s': '%s'", remDir, err)
		return err
	}

	dstFile, err := s.share.Create(dstPath)
	if err != nil {
		appCtx.Log().Errorf("Unable to create remote file: %s", err)
		return err
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	if err != nil {
		appCtx.Log().Errorf("Unable to make copy: %s", err)
		return err
	}

	appCtx.Log().Infof("%s crated", dstPath)

	for dst, src := range links {
		remDir = path.Dir(dst)
		err = s.share.MkdirAll(remDir, os.ModeDir)
		if err != nil {
			appCtx.Log().Errorf("Unable to create remote directory '%s': '%s'", remDir, err)
			return err
		}
		err = s.share.Symlink(src, dst)
		if err != nil {
			appCtx.Log().Errorf("Unable to make symlink: %s", err)
			return err
		}
	}

	return nil
}

func (s *SMB) ControlFiles(appCtx *appctx.AppContext, ofsPartsList []string) error {

	var errs []error
	curDate := time.Now()

	for _, period := range []string{"daily", "weekly", "monthly"} {
		for _, ofsPart := range ofsPartsList {
			bakDir := path.Join(s.BackupPath, ofsPart, period)
			files, err := s.share.ReadDir(bakDir)
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
					retentionDate = fileDate.AddDate(0, 0, s.Retention.Days)
				case "weekly":
					retentionDate = fileDate.AddDate(0, 0, s.Retention.Weeks*7)
				case "monthly":
					retentionDate = fileDate.AddDate(0, s.Retention.Months, 0)
				}

				retentionDate = retentionDate.Truncate(24 * time.Hour)
				if curDate.After(retentionDate) {
					err = s.share.Remove(path.Join(bakDir, file.Name()))
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

func (s *SMB) Close() error {
	_ = s.share.Umount()
	return s.session.Logoff()
}
