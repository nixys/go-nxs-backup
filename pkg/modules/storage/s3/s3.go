package s3

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	appctx "github.com/nixys/nxs-go-appctx/v2"

	"nxs-backup/interfaces"
	"nxs-backup/misc"
	. "nxs-backup/modules/storage"
)

type S3 struct {
	client     *minio.Client
	bucketName string
	backupPath string
	Retention
}

type Params struct {
	BucketName      string `conf:"bucket_name" conf_extraopts:"required"`
	AccessKeyID     string `conf:"access_key_id"`
	SecretAccessKey string `conf:"secret_access_key"`
	Endpoint        string `conf:"endpoint" conf_extraopts:"required"`
	Region          string `conf:"region" conf_extraopts:"required"`
}

func Init(params Params) (*S3, error) {

	s3Client, err := minio.New(params.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(params.AccessKeyID, params.SecretAccessKey, ""),
		Secure: true,
	})
	if err != nil {
		return nil, err
	}

	return &S3{
		client:     s3Client,
		bucketName: params.BucketName,
	}, nil
}

func (s *S3) IsLocal() int { return 0 }

func (s *S3) SetBackupPath(path string) {
	s.backupPath = path
}

func (s *S3) SetRetention(r Retention) {
	s.Retention = r
}

func (s *S3) DeliveryBackup(appCtx *appctx.AppContext, tmpBackupFile, ofs, bakType string) error {
	var bakRemPaths, mtdRemPaths []string

	source, err := os.Open(tmpBackupFile)
	if err != nil {
		return err
	}
	defer func() { _ = source.Close() }()

	sourceStat, err := source.Stat()
	if err != nil {
		return err
	}

	if bakType == misc.IncBackupType {
		bakRemPaths, mtdRemPaths = GetIncBackupDstList(tmpBackupFile, ofs, s.backupPath)
	} else {
		bakRemPaths = GetDescBackupDstList(tmpBackupFile, ofs, s.backupPath, s.Retention)
	}

	if len(mtdRemPaths) > 0 {
		var mtdSrc *os.File
		mtdSrc, err = os.Open(tmpBackupFile + ".inc")
		if err != nil {
			return err
		}
		defer func() { _ = mtdSrc.Close() }()

		var mtdSrcStat os.FileInfo
		mtdSrcStat, err = source.Stat()
		if err != nil {
			return err
		}

		for _, bucketPath := range mtdRemPaths {
			_, err = s.client.PutObject(context.Background(), s.bucketName, bucketPath, mtdSrc, mtdSrcStat.Size(), minio.PutObjectOptions{ContentType: "application/octet-stream"})
			if err != nil {
				return err
			}
			appCtx.Log().Infof("Successfully uploaded object '%s' in bucket %s", bucketPath, s.bucketName)
		}
	}

	for _, bucketPath := range bakRemPaths {
		_, err = s.client.PutObject(context.Background(), s.bucketName, bucketPath, source, sourceStat.Size(), minio.PutObjectOptions{ContentType: "application/octet-stream"})
		if err != nil {
			return err
		}
		appCtx.Log().Infof("Successfully uploaded object '%s' in bucket %s", bucketPath, s.bucketName)
	}

	return nil
}

func (s *S3) DeleteOldBackups(appCtx *appctx.AppContext, ofsPartsList []string, bakType string, full bool) error {

	var errs *multierror.Error
	var objsToDel []minio.ObjectInfo

	objCh := make(chan minio.ObjectInfo)
	curDate := time.Now()

	// Send object that are needed to be removed to objCh
	go func() {
		defer close(objCh)
		for _, ofs := range ofsPartsList {
			backupDir := path.Join(s.backupPath, ofs)
			basePath := strings.TrimPrefix(backupDir, "/")

			for object := range s.client.ListObjects(context.Background(), s.bucketName, minio.ListObjectsOptions{Recursive: true, Prefix: basePath}) {
				if object.Err != nil {
					appCtx.Log().Errorf("Failed get objects: '%s'", object.Err)
					errs = multierror.Append(errs, object.Err)
				}

				if bakType == misc.IncBackupType {
					if full {
						objsToDel = append(objsToDel, object)
					} else {
						intMoy, _ := strconv.Atoi(misc.GetDateTimeNow("moy"))
						lastMonth := intMoy - s.Months

						var year string
						if lastMonth > 0 {
							year = misc.GetDateTimeNow("year")
						} else {
							year = misc.GetDateTimeNow("previous_year")
							lastMonth += 12
						}
						rx := regexp.MustCompile(year + "/month_\\d\\d")
						if rx.MatchString(object.Key) {
							dirParts := strings.Split(path.Base(object.Key), "_")
							dirMonth, _ := strconv.Atoi(dirParts[1])
							if dirMonth < lastMonth {
								objsToDel = append(objsToDel, object)
							}
						}
					}
				} else {
					fileDate := object.LastModified
					var retentionDate time.Time

					if strings.Contains(object.Key, "daily") {
						retentionDate = fileDate.AddDate(0, 0, s.Retention.Days)
					}
					if strings.Contains(object.Key, "weekly") {
						retentionDate = fileDate.AddDate(0, 0, s.Retention.Weeks*7)
					}
					if strings.Contains(object.Key, "monthly") {
						retentionDate = fileDate.AddDate(0, s.Retention.Months, 0)
					}
					retentionDate = retentionDate.Truncate(24 * time.Hour)
					if curDate.After(retentionDate) {
						objsToDel = append(objsToDel, object)
					}
				}
			}
		}
	}()

	for rErr := range s.client.RemoveObjects(context.Background(), s.bucketName, objCh, minio.RemoveObjectsOptions{GovernanceBypass: true}) {
		appCtx.Log().Errorf("Error detected during object deletion: '%s'", rErr)
		errs = multierror.Append(errs, rErr.Err)
	}

	return errs.ErrorOrNil()
}

func (s *S3) GetFileReader(ofsPath string) (io.Reader, error) {
	o, err := s.client.GetObject(context.Background(), s.bucketName, ofsPath, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = o.Close() }()

	var buf []byte
	buf, err = ioutil.ReadAll(o)
	if err != nil {
		return nil, err
	}

	return bytes.NewReader(buf), err
}

func (s *S3) Close() error {
	return nil
}

func (s *S3) Clone() interfaces.Storage {
	cl := *s
	return &cl
}
