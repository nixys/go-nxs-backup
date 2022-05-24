package backup

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	appctx "github.com/nixys/nxs-go-appctx/v2"

	"nxs-backup/interfaces"
	"nxs-backup/misc"
)

type DescFilesJob struct {
	Name                 string
	TmpDir               string
	SafetyBackup         bool
	DeferredCopyingLevel int
	Sources              []DescFilesSource
	Storages             []interfaces.Storage
	NeedToMakeBackup     bool
	OfsPartsList
	DumpedObjects
}

type OfsPartsList []string

type DescFilesSource struct {
	Targets []TargetOfs
	Gzip    bool
}

type TargetOfs map[string]string

func (j DescFilesJob) JobName() string {
	return j.Name
}

func (j DescFilesJob) JobType() string {
	return "desc_files"
}

func (j DescFilesJob) DoBackup(appCtx *appctx.AppContext) (errs []error) {

	if j.SafetyBackup {
		defer func(j DescFilesJob) {
			err := j.controlOldBackups(appCtx)
			if err != nil {
				errs = append(errs, err...)
			}
		}(j)
	} else {
		err := j.controlOldBackups(appCtx)
		if err != nil {
			errs = append(errs, err...)
			return
		}
	}
	if j.NeedToMakeBackup {

		tmpDirPath := path.Join(j.TmpDir, fmt.Sprintf("%s_%s", j.JobType(), misc.GetDateTimeNow("")))
		err := os.MkdirAll(tmpDirPath, os.ModePerm)
		if err != nil {
			appCtx.Log().Error(err)
			return []error{err}
		}

		j.DumpedObjects = make(map[string]string)

		for _, source := range j.Sources {

			for _, target := range source.Targets {

				for ofsPart, ofs := range target {

					tmpBackupFullPath := misc.GetBackupFullPath(j.TmpDir, ofsPart, "tar", "", source.Gzip)
					err = createBackup(tmpBackupFullPath, ofs, source.Gzip)
					if err != nil {
						errs = append(errs, err)
						continue
					} else {
						appCtx.Log().Infof("created temp backups %s by job %s", tmpBackupFullPath, j.Name)
					}

					j.DumpedObjects[ofsPart] = tmpBackupFullPath

					if j.DeferredCopyingLevel <= 0 {
						errLst := j.DumpedObjects.Delivery(appCtx, j.Storages)
						errs = append(errs, errLst...)
						j.DumpedObjects = make(map[string]string)
					}
				}
				if j.DeferredCopyingLevel == 1 {
					errLst := j.DumpedObjects.Delivery(appCtx, j.Storages)
					errs = append(errs, errLst...)
					j.DumpedObjects = make(map[string]string)
				}
			}
			if j.DeferredCopyingLevel >= 2 {
				errLst := j.DumpedObjects.Delivery(appCtx, j.Storages)
				errs = append(errs, errLst...)
				j.DumpedObjects = make(map[string]string)
			}
		}

		files, _ := ioutil.ReadDir(tmpDirPath)
		if len(files) == 0 {
			err = os.Remove(tmpDirPath)
			if err != nil {
				errs = append(errs, err)
			}
		}
	} else {
		appCtx.Log().Infof("According to the backup plan today new backups are not created for job %s", j.Name)
	}

	return
}

func (j DescFilesJob) controlOldBackups(appCtx *appctx.AppContext) (errs []error) {

	for _, storage := range j.Storages {
		err := storage.ControlFiles(appCtx, j.OfsPartsList)
		if err != nil {
			errs = append(errs, err...)
		}
	}
	return
}

func createBackup(tmpBackupPath, ofs string, gZip bool) (err error) {
	backupFile, err := os.Create(tmpBackupPath)
	if err != nil {
		return
	}

	var backupWriter io.WriteCloser
	if gZip {
		backupWriter = gzip.NewWriter(backupFile)
	} else {
		backupWriter = backupFile
	}
	defer backupWriter.Close()

	tarWriter := tar.NewWriter(backupWriter)
	defer tarWriter.Close()

	err = writeDirectory(ofs, tarWriter, filepath.Dir(ofs))
	return
}

func writeDirectory(directory string, tarWriter *tar.Writer, subPath string) error {

	files, err := ioutil.ReadDir(directory)
	if err != nil {
		return err
	}

	if len(files) == 0 {
		dirInfo, err := os.Stat(directory)
		if err != nil {
			return err
		}
		err = writeTar(directory, tarWriter, dirInfo, subPath)
		if err != nil {
			return err
		}
	}

	for _, file := range files {
		currentPath := filepath.Join(directory, file.Name())
		if file.IsDir() {
			err := writeDirectory(currentPath, tarWriter, subPath)
			if err != nil {
				return err
			}
		} else {
			err = writeTar(currentPath, tarWriter, file, subPath)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func writeTar(path string, tarWriter *tar.Writer, fileInfo os.FileInfo, subPath string) error {
	var link string
	if fileInfo.Mode()&os.ModeSymlink == os.ModeSymlink {
		var err error
		if link, err = os.Readlink(path); err != nil {
			return err
		}
	}

	if fileInfo.Mode()&os.ModeSocket == os.ModeSocket {
		return nil
	}

	header, err := tar.FileInfoHeader(fileInfo, link)
	if err != nil {
		return err
	}
	header.Name = path[len(subPath):]

	err = tarWriter.WriteHeader(header)
	if err != nil {
		return err
	}

	if !fileInfo.Mode().IsRegular() {
		return nil
	}

	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = io.Copy(tarWriter, file)
	if err != nil {
		return err
	}

	return err
}
