package backup

import (
	"fmt"
	"github.com/minio/minio-go/v7"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"io/ioutil"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/minio/minio-go/v7/pkg/credentials"
	"nxs-backup/interfaces"
	"nxs-backup/misc"
	"nxs-backup/modules/storage"
)

type JobSettings struct {
	JobName              string
	JobType              string
	TmpDir               string
	DumpCmd              string
	SafetyBackup         bool
	DeferredCopyingLevel int
	IncMonthsToStore     int
	Sources              []SourceSettings
	StorageNames         []string
}

type StorageSettings struct {
	Enable    bool
	Type      string
	Retention RetentionSettings
	S3Options
	LocalOptions
	SFTPOptions
}

type S3Options struct {
	BackupPath      string
	BucketName      string
	AccessKeyID     string
	SecretAccessKey string
	Endpoint        string
	Region          string
}

type SFTPOptions struct {
	BackupPath     string
	User           string
	Host           string
	Port           int
	Password       string
	KeyFile        string
	ConnectTimeout int
}

type LocalOptions struct {
	BackupPath string
}

type RetentionSettings struct {
	Days   int
	Weeks  int
	Months int
}

type SourceSettings struct {
	SpecialKeys        string
	Targets            []string
	TargetDbs          []string
	TargetCollections  []string
	Excludes           []string
	ExcludeDbs         []string
	ExcludeCollections []string
	Gzip               bool
	SkipBackupRotate   bool
	ConnectSettings
}

type ConnectSettings struct {
	AuthFile   string
	DBHost     string
	DBPort     string
	Socket     string
	DBUser     string
	DBPassword string
	PathToConf string
}

func JobsInit(js []JobSettings, ss map[string]StorageSettings) (jobs []interfaces.Job, errs []error) {

	for _, j := range js {

		// jobs validation
		if len(j.JobName) == 0 {
			errs = append(errs, fmt.Errorf("empty job name is unacceptable"))
			continue
		}
		if !misc.Contains(misc.AllowedJobTypes, j.JobType) {
			errs = append(errs, fmt.Errorf("unknown job type \"%s\". Allowd types: %s", j.JobType, misc.AllowedJobTypes))
			continue
		}

		var sts []interfaces.Storage
		needToMakeBackup := false

		for _, sn := range j.StorageNames {
			// storages validation
			s, ok := ss[sn]
			if !ok {
				errs = append(errs, fmt.Errorf("unknown storage name: %s", sn))
				continue
			}
			if !misc.Contains(misc.AllowedStorageTypes, s.Type) {
				errs = append(errs, fmt.Errorf("unknown storage type \"%s\". Allowd types: %s", s.Type, strings.Join(misc.AllowedStorageTypes, ", ")))
			}
			if s.Retention.Days < 0 || s.Retention.Weeks < 0 || s.Retention.Months < 0 {
				errs = append(errs, fmt.Errorf("retention period can't be negative"))
			}

			if s.Enable {
				if misc.NeedToMakeBackup(s.Retention.Days, s.Retention.Weeks, s.Retention.Months) {
					needToMakeBackup = true
				}
				switch s.Type {
				case "s3":
					s3Client, err := minio.New(s.S3Options.Endpoint, &minio.Options{
						Creds:  credentials.NewStaticV4(s.S3Options.AccessKeyID, s.S3Options.SecretAccessKey, ""),
						Secure: true,
					})
					if err != nil {
						errs = append(errs, err)
						continue
					}

					sts = append(sts, &storage.S3{
						Retention: storage.Retention(s.Retention),
						Client:    s3Client,
						S3Options: storage.S3Options{
							BackupPath: s.S3Options.BackupPath,
							BucketName: s.S3Options.BucketName,
						},
					})

				case "ssh", "sftp":
					sshConfig := &ssh.ClientConfig{
						User:            s.SFTPOptions.User,
						Auth:            []ssh.AuthMethod{},
						HostKeyCallback: ssh.InsecureIgnoreHostKey(),
						Timeout:         time.Duration(s.SFTPOptions.ConnectTimeout) * time.Second,
						ClientVersion:   "SSH-2.0-" + "nxs-backup/" + misc.VERSION,
					}

					if s.SFTPOptions.Password != "" {
						sshConfig.Auth = append(sshConfig.Auth, ssh.Password(s.SFTPOptions.Password))
					}

					// Load key file if specified
					if s.SFTPOptions.KeyFile != "" {
						key, err := ioutil.ReadFile(s.SFTPOptions.KeyFile)
						if err != nil {
							errs = append(errs, fmt.Errorf("failed to read private key file: %w", err))
							continue
						}
						signer, err := ssh.ParsePrivateKey(key)
						if err != nil {
							errs = append(errs, fmt.Errorf("failed to parse private key file: %w", err))
							continue
						}
						sshConfig.Auth = append(sshConfig.Auth, ssh.PublicKeys(signer))
					}

					sshConn, err := ssh.Dial("tcp", fmt.Sprintf("%s:%d", s.SFTPOptions.Host, s.SFTPOptions.Port), sshConfig)
					if err != nil {
						errs = append(errs, fmt.Errorf("couldn't connect SSH: %w", err))
					}

					sftpClient, err := sftp.NewClient(sshConn)
					if err != nil {
						_ = sshConn.Close()
						errs = append(errs, fmt.Errorf("couldn't initialise SFTP: %w", err))
						continue
					}

					sts = append(sts, &storage.SFTP{
						Retention:  storage.Retention(s.Retention),
						Client:     sftpClient,
						BackupPath: s.SFTPOptions.BackupPath,
					})
				case "local":
					sts = append(sts, &storage.Local{
						BackupPath: s.LocalOptions.BackupPath,
						Retention:  storage.Retention(s.Retention),
					})
				}
			}
		}
		if len(sts) > 1 {
			sort.Sort(interfaces.StorageSortByLocal(sts))
		}

		switch j.JobType {
		case "desc_files":
			var (
				srcs         []DescFilesSource
				ofsPartsList OfsPartsList
			)
			for _, s := range j.Sources {

				var tgts []TargetOfs
				for _, targetPattern := range s.Targets {

					for strings.HasSuffix(targetPattern, "/") {
						targetPattern = strings.TrimSuffix(targetPattern, "/")
					}

					targetOfsList, err := filepath.Glob(targetPattern)
					if err != nil {
						errs = append(errs, fmt.Errorf("%s. Pattern: %s", err, targetPattern))
						continue
					}

					targetOfsMap := make(map[string]string)
					for _, ofs := range targetOfsList {

						excluded := false
						for _, exclPattern := range s.Excludes {

							match, err := filepath.Match(exclPattern, ofs)
							if err != nil {
								errs = append(errs, fmt.Errorf("%s. Pattern: %s", err, exclPattern))
								continue
							}
							if match {
								excluded = true
								break
							}
						}

						if !excluded {
							ofsPart := misc.GetOfsPart(targetPattern, ofs)
							targetOfsMap[ofsPart] = ofs
							ofsPartsList = append(ofsPartsList, ofsPart)
						}
					}

					tgts = append(tgts, targetOfsMap)
				}

				srcs = append(srcs, DescFilesSource{
					Targets: tgts,
					Gzip:    s.Gzip,
				})
			}

			jobs = append(jobs, DescFilesJob{
				Name:                 j.JobName,
				TmpDir:               j.TmpDir,
				SafetyBackup:         j.SafetyBackup,
				DeferredCopyingLevel: j.DeferredCopyingLevel,
				Sources:              srcs,
				Storages:             sts,
				NeedToMakeBackup:     needToMakeBackup,
				OfsPartsList:         ofsPartsList,
			})
		// "external" as default
		default:

		}
	}

	return
}
