package sftp

import (
	"fmt"
	"golang.org/x/crypto/ssh"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	appctx "github.com/nixys/nxs-go-appctx/v2"
	"github.com/pkg/sftp"

	"nxs-backup/interfaces"
	"nxs-backup/misc"
	. "nxs-backup/modules/storage"
)

type SFTP struct {
	Client     *sftp.Client
	BackupPath string
	Retention
}

type Params struct {
	User           string
	Host           string
	Port           int
	Password       string
	KeyFile        string
	ConnectTimeout time.Duration
}

func Init(params Params) (*SFTP, error) {

	sshConfig := &ssh.ClientConfig{
		User:            params.User,
		Auth:            []ssh.AuthMethod{},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         params.ConnectTimeout * time.Second,
		ClientVersion:   "SSH-2.0-" + "nxs-backup/" + misc.VERSION,
	}

	if params.Password != "" {
		sshConfig.Auth = append(sshConfig.Auth, ssh.Password(params.Password))
	}

	// Load key file if specified
	if params.KeyFile != "" {
		key, err := ioutil.ReadFile(params.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read private key file: %w", err)
		}
		signer, err := ssh.ParsePrivateKey(key)
		if err != nil {
			return nil, fmt.Errorf("failed to parse private key file: %w", err)
		}
		sshConfig.Auth = append(sshConfig.Auth, ssh.PublicKeys(signer))
	}

	sshConn, err := ssh.Dial("tcp", fmt.Sprintf("%s:%d", params.Host, params.Port), sshConfig)
	if err != nil {
		return nil, fmt.Errorf("couldn't connect SSH: %w", err)
	}

	sftpClient, err := sftp.NewClient(sshConn)
	if err != nil {
		_ = sshConn.Close()
		return nil, fmt.Errorf("couldn't initialise SFTP: %w", err)
	}

	return &SFTP{
		Client: sftpClient,
	}, nil

}

func (s *SFTP) IsLocal() int { return 0 }

func (s *SFTP) SetBackupPath(path string) {
	s.BackupPath = path
}

func (s *SFTP) SetRetention(r Retention) {
	s.Retention = r
}

func (s *SFTP) DeliveryBackup(appCtx *appctx.AppContext, tmpBackup, ofs, bakType string) error {

	srcFile, err := os.Open(tmpBackup)
	if err != nil {
		appCtx.Log().Errorf("Unable to open tmp backup: '%s'", err)
		return err
	}
	defer srcFile.Close()

	dstPath, links, err := GetDescBackupDstAndLinks(filepath.Base(tmpBackup), ofs, s.BackupPath, s.Retention)
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
		appCtx.Log().Errorf("Unable to create remote file: %s", err)
		return err
	}
	defer dstFile.Close()

	bytes, err := io.Copy(dstFile, srcFile)
	if err != nil {
		appCtx.Log().Errorf("Unable to upload file: %s", err)
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
			appCtx.Log().Errorf("Unable to create symlink: %s", err)
			return err
		}
	}

	return nil
}

func (s *SFTP) DeleteOldBackups(appCtx *appctx.AppContext, ofsPartsList []string, bakType string) error {

	var errs []error
	curDate := time.Now()
	// TODO delete old inc backups

	for _, period := range []string{"daily", "weekly", "monthly"} {
		for _, ofsPart := range ofsPartsList {
			bakDir := filepath.Join(s.BackupPath, ofsPart, period)
			files, err := s.Client.ReadDir(bakDir)
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
					err = s.Client.Remove(filepath.Join(bakDir, file.Name()))
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

func (s *SFTP) GetFile(ofsPath string) (fs.File, error) {
	//TODO implement me
	panic("implement me")
}

func (s *SFTP) Close() error {
	return s.Client.Close()
}

func (s *SFTP) Clone() interfaces.Storage {
	cl := *s
	return &cl
}
