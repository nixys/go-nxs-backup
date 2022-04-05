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
	JobName              string
	TmpDir               string
	DumpCmd              string
	SafetyBackup         bool
	DeferredCopyingLevel int
	IncMonthsToStore     int
	Sources              []DescFilesSource
	Storages             []interfaces.Storage
}

type DescFilesSource struct {
	Targets  []string
	Excludes []string
	Gzip     bool
}

func (j DescFilesJob) GetJobType() string {
	return "desc_files"
}

func (j DescFilesJob) MakeBackup(appCtx *appctx.AppContext) (errs []error) {

	//cc := appCtx.CustomCtx().(*ctx.Ctx)
	tmpDirPath := path.Join(j.TmpDir, fmt.Sprintf("%s_%s", j.GetJobType(), misc.GetDateTimeNow("")))
	err := os.MkdirAll(tmpDirPath, os.ModePerm)
	if err != nil {
		appCtx.Log().Error(err)
		return []error{err}
	}

	dumpedOfs := make(map[string]string)

	for _, source := range j.Sources {

		for _, tPattern := range source.Targets {
			targetFiles, err := filepath.Glob(tPattern)
			if err != nil {
				e := fmt.Errorf("%s. Pattern: %s", err, tPattern)
				appCtx.Log().Error(e)
				errs = append(errs, e)
				continue
			}

			for _, ofs := range targetFiles {
				excluded := false
				for _, exPattern := range source.Excludes {
					match, err := filepath.Match(exPattern, ofs)
					if err != nil {
						e := fmt.Errorf("%s. Pattern: %s", err, exPattern)
						appCtx.Log().Error(e)
						errs = append(errs, e)
						continue
					}
					if match {
						excluded = true
						break
					}
				}
				if excluded {
					continue
				}

				backupFileName := misc.GetBackupFileName(tPattern, ofs)
				tmpBackupFullPath := misc.GetBackupFullPath(tmpDirPath, backupFileName, "tar", "", source.Gzip)

				backupFile, err := os.Create(tmpBackupFullPath)
				if err != nil {
					errs = append(errs, err)
					continue
				}
				defer backupFile.Close()

				var backupWriter io.WriteCloser
				if source.Gzip {
					backupWriter = gzip.NewWriter(backupFile)
				} else {
					backupWriter = backupFile
				}
				defer backupWriter.Close()

				tarWriter := tar.NewWriter(backupWriter)
				defer tarWriter.Close()

				err = writeDirectory(ofs, tarWriter, filepath.Dir(ofs))
				if err != nil {
					errs = append(errs, err)
				} else {
					dumpedOfs[backupFileName] = tmpBackupFullPath
				}
				if j.DeferredCopyingLevel <= 0 {
					continue // do rotate
				}
			}
			if j.DeferredCopyingLevel == 1 {
				continue
			}
		}
		if j.DeferredCopyingLevel >= 2 {
			continue
		}
	}
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
