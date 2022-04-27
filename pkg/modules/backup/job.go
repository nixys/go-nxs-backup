package backup

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/minio/minio-go/v7"
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
	Storages             []StorageSettings
}

type StorageSettings struct {
	Enable    bool
	Type      string
	Retention RetentionSettings
	S3StorageOptions
	LocalStorageOptions
}

type S3StorageOptions struct {
	BucketName      string
	AccessKeyID     string
	SecretAccessKey string
	Endpoint        string
	Region          string
	BackupPath      string
}

type LocalStorageOptions struct {
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

func JobsInit(js []JobSettings) (jobs []interfaces.Job, errs []error) {

	for _, j := range js {

		var sts []interfaces.Storage
		needToMakeBackup := false

		for _, s := range j.Storages {
			if s.Enable {
				if misc.NeedToMakeBackup(s.Retention.Days, s.Retention.Weeks, s.Retention.Months) {
					needToMakeBackup = true
				}
				switch s.Type {
				case "s3":
					s3Client, err := minio.New(s.S3StorageOptions.Endpoint, &minio.Options{
						Creds:  credentials.NewStaticV4(s.S3StorageOptions.AccessKeyID, s.S3StorageOptions.SecretAccessKey, ""),
						Secure: true,
					})
					if err != nil {
						errs = append(errs, err)
					}

					sts = append(sts, &storage.S3{
						Retention: storage.Retention(s.Retention),
						Client:    s3Client,
						S3Options: storage.S3Options{
							BackupPath: s.S3StorageOptions.BackupPath,
							BucketName: s.S3StorageOptions.BucketName,
						},
					})
				case "local":
					sts = append(sts, &storage.Local{
						BackupPath: s.LocalStorageOptions.BackupPath,
						Retention:  storage.Retention(s.Retention),
					})
				}
			}
		}
		if len(sts) > 1 {
			sort.Sort(interfaces.SortByLocal(sts))
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
