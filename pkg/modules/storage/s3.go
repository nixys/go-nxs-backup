package storage

import (
	"context"
	"github.com/minio/minio-go/v7"
	appctx "github.com/nixys/nxs-go-appctx/v2"
	"nxs-backup/misc"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

type S3 struct {
	Client *minio.Client
	S3Options
	Retention
}

type S3Options struct {
	BackupPath string
	BucketName string
}

func (s *S3) IsLocal() int { return 0 }

func (s *S3) CopyFile(appCtx *appctx.AppContext, tmpBackup, ofs string, _ bool) error {

	source, err := os.Open(tmpBackup)
	if err != nil {
		return err
	}
	defer source.Close()

	sourceStat, err := source.Stat()
	if err != nil {
		return err
	}

	bucketPaths := s.getDstList(filepath.Base(tmpBackup), ofs)

	for _, bucketPath := range bucketPaths {
		n, err := s.Client.PutObject(context.Background(), s.BucketName, bucketPath, source, sourceStat.Size(), minio.PutObjectOptions{ContentType: "application/octet-stream"})
		if err != nil {
			return err
		}
		appCtx.Log().Infof("Successfully uploaded '%d' bytes, created object '%s' in bucket %s", n.Size, n.Key, n.Bucket)
	}

	return nil
}

func (s *S3) ListFiles() (err error) {
	return
}

func (s *S3) ControlFiles(appCtx *appctx.AppContext, ofsPartsList []string) (errs []error) {

	objCh := make(chan minio.ObjectInfo)
	curDate := time.Now()

	objMap, err := s.getObjectsPeriodicMap(ofsPartsList)
	if err != nil {
		errs = append(errs, err)
		return
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
		appCtx.Log().Errorf("Error detected during deletion: '%s'", rErr)
		errs = append(errs, err)
	}

	return
}

func (s *S3) getDstList(bakFile, ofs string) (dst []string) {

	basePath := strings.TrimPrefix(path.Join(s.BackupPath, ofs), "/")

	if misc.GetDateTimeNow("dom") == misc.MonthlyBackupDay && s.Months > 0 {
		dst = append(dst, path.Join(basePath, "monthly", bakFile))
	}
	if misc.GetDateTimeNow("dow") == misc.WeeklyBackupDay && s.Weeks > 0 {
		dst = append(dst, path.Join(basePath, "weekly", bakFile))
	}
	if s.Days > 0 {
		dst = append(dst, path.Join(basePath, "daily", bakFile))
	}

	return
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
