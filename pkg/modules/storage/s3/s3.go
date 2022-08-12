package s3

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	appctx "github.com/nixys/nxs-go-appctx/v2"

	"nxs-backup/interfaces"
	. "nxs-backup/modules/storage"
)

type S3 struct {
	Client     *minio.Client
	BucketName string
	BackupPath string
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
		Client:     s3Client,
		BucketName: params.BucketName,
	}, nil
}

func (s *S3) IsLocal() int { return 0 }

func (s *S3) SetBackupPath(path string) {
	s.BackupPath = path
}

func (s *S3) SetRetention(r Retention) {
	s.Retention = r
}

func (s *S3) DeliveryDescBackup(appCtx *appctx.AppContext, tmpBackup, ofs string) error {

	source, err := os.Open(tmpBackup)
	if err != nil {
		return err
	}
	defer source.Close()

	sourceStat, err := source.Stat()
	if err != nil {
		return err
	}

	bucketPaths := GetDescBackupDstList(filepath.Base(tmpBackup), ofs, s.BackupPath, s.Retention)

	for _, bucketPath := range bucketPaths {
		n, err := s.Client.PutObject(context.Background(), s.BucketName, bucketPath, source, sourceStat.Size(), minio.PutObjectOptions{ContentType: "application/octet-stream"})
		if err != nil {
			return err
		}
		appCtx.Log().Infof("Successfully created object '%s' in bucket %s", n.Key, n.Bucket)
	}

	return nil
}

func (s *S3) DeliveryIncBackup(appCtx *appctx.AppContext, tmpBackupPath, ofs string, init bool) error {
	//TODO implement me
	panic("implement me")
}

func (s *S3) DeliveryIncBackupMetadata(appCtx *appctx.AppContext, tmpBackupMetadata, ofs string, init bool) (err error) {
	//TODO implement me
	panic("implement me")
}

func (s *S3) DeleteOldDescBackups(appCtx *appctx.AppContext, ofsPartsList []string) error {

	var errs []error
	objCh := make(chan minio.ObjectInfo)
	curDate := time.Now()

	objMap, err := s.getObjectsPeriodicMap(ofsPartsList)
	if err != nil {
		appCtx.Log().Errorf("Failed get objects: '%s'", err)
		return err
	}

	// Send object that are needed to be removed to objCh
	go func() {
		defer close(objCh)
		for _, period := range []string{"daily", "weekly", "monthly"} {

			for _, obj := range objMap[period] {

				fileDate := obj.LastModified
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
					objCh <- obj
					appCtx.Log().Infof("Object '%s' to be removed from bucket '%s'", obj.Key, s.BucketName)
				}
			}
		}
	}()

	for rErr := range s.Client.RemoveObjects(context.Background(), s.BucketName, objCh, minio.RemoveObjectsOptions{GovernanceBypass: true}) {
		appCtx.Log().Errorf("Error detected during object deletion: '%s'", rErr)
		errs = append(errs, rErr.Err)
	}

	if len(errs) > 0 {
		return fmt.Errorf("some errors on file deletion")
	}

	return nil
}

func (s *S3) getObjectsPeriodicMap(ofsPartsList []string) (objs map[string][]minio.ObjectInfo, err error) {
	objs = make(map[string][]minio.ObjectInfo)

	for _, ofs := range ofsPartsList {
		basePath := strings.TrimPrefix(path.Join(s.BackupPath, ofs), "/")
		for object := range s.Client.ListObjects(context.Background(), s.BucketName, minio.ListObjectsOptions{Recursive: true, Prefix: basePath}) {
			if object.Err != nil {
				err = object.Err
				return
			}

			if strings.Contains(object.Key, "daily") {
				objs["daily"] = append(objs["daily"], object)
			}
			if strings.Contains(object.Key, "weekly") {
				objs["weekly"] = append(objs["weekly"], object)
			}
			if strings.Contains(object.Key, "monthly") {
				objs["monthly"] = append(objs["monthly"], object)
			}
		}
	}
	return
}

func (s *S3) GetFile(ofsPath string) (fs.File, error) {
	//TODO implement me
	panic("implement me")
}

func (s *S3) Close() error {
	return nil
}

func (s *S3) Clone() interfaces.Storage {
	cl := *s
	return &cl
}
