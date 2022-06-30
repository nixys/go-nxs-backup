package desc_files

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"

	appctx "github.com/nixys/nxs-go-appctx/v2"

	"nxs-backup/interfaces"
	"nxs-backup/misc"
)

type Job struct {
	Name                 string
	TmpDir               string
	NeedToMakeBackup     bool
	SafetyBackup         bool
	DeferredCopyingLevel int
	Storages             interfaces.Storages
	Sources              []Source
	DumpedObjects        map[string]string
	OfsPartsList         []string
}

type Source struct {
	Targets []TargetOfs
	Gzip    bool
}

type TargetOfs map[string]string

type Params struct {
	Name                 string
	TmpDir               string
	NeedToMakeBackup     bool
	SafetyBackup         bool
	DeferredCopyingLevel int
	Storages             interfaces.Storages
	Sources              []SourceParams
}

type SourceParams struct {
	Gzip     bool
	Targets  []string
	Excludes []string
}

func Init(p Params) (*Job, error) {

	job := &Job{
		Name:                 p.Name,
		TmpDir:               p.TmpDir,
		NeedToMakeBackup:     p.NeedToMakeBackup,
		SafetyBackup:         p.SafetyBackup,
		DeferredCopyingLevel: p.DeferredCopyingLevel,
		Storages:             p.Storages,
	}

	var (
		sources      []Source
		ofsPartsList []string
	)
	for _, s := range p.Sources {

		var targets []TargetOfs
		for _, targetPattern := range s.Targets {

			for strings.HasSuffix(targetPattern, "/") {
				targetPattern = strings.TrimSuffix(targetPattern, "/")
			}

			targetOfsList, err := filepath.Glob(targetPattern)
			if err != nil {
				return nil, fmt.Errorf("%s. Pattern: %s", err, targetPattern)
			}

			targetOfsMap := make(map[string]string)
			for _, ofs := range targetOfsList {

				excluded := false
				for _, exclPattern := range s.Excludes {

					match, err := filepath.Match(exclPattern, ofs)
					if err != nil {
						return nil, fmt.Errorf("%s. Pattern: %s", err, exclPattern)
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

			targets = append(targets, targetOfsMap)
		}

		sources = append(sources, Source{
			Targets: targets,
			Gzip:    s.Gzip,
		})
	}

	job.Sources = sources
	job.OfsPartsList = ofsPartsList

	return job, nil
}

func (j *Job) GetJobName() string {
	return j.Name
}

func (j *Job) GetJobType() string {
	return "files"
}

func (j *Job) DoBackup(appCtx *appctx.AppContext) (errs []error) {

	if j.SafetyBackup {
		defer func() {
			err := j.Storages.CleanupOldBackups(appCtx, j.OfsPartsList)
			if err != nil {
				errs = append(errs, err...)
			}
		}()
	} else {
		err := j.Storages.CleanupOldBackups(appCtx, j.OfsPartsList)
		if err != nil {
			errs = append(errs, err...)
			return
		}
	}

	if !j.NeedToMakeBackup {
		appCtx.Log().Infof("According to the backup plan today new backups are not created for job %s", j.Name)
		return
	}

	tmpDirPath := path.Join(j.TmpDir, fmt.Sprintf("%s_%s", j.GetJobType(), misc.GetDateTimeNow("")))
	err := os.MkdirAll(tmpDirPath, os.ModePerm)
	if err != nil {
		appCtx.Log().Error(err)
		return []error{err}
	}

	j.DumpedObjects = make(map[string]string)

	for _, source := range j.Sources {

		for _, target := range source.Targets {

			for ofsPart, ofs := range target {

				tmpBackupFullPath := misc.GetBackupFullPath(tmpDirPath, ofsPart, "tar", "", source.Gzip)
				err = createBackup(tmpBackupFullPath, ofs, source.Gzip)
				if err != nil {
					errs = append(errs, err)
					continue
				} else {
					appCtx.Log().Infof("created temp backups %s by job %s", tmpBackupFullPath, j.Name)
				}

				j.DumpedObjects[ofsPart] = tmpBackupFullPath

				if j.DeferredCopyingLevel <= 0 {
					errLst := j.Storages.Delivery(appCtx, j.DumpedObjects)
					errs = append(errs, errLst...)
					j.DumpedObjects = make(map[string]string)
				}
			}
			if j.DeferredCopyingLevel == 1 {
				errLst := j.Storages.Delivery(appCtx, j.DumpedObjects)
				errs = append(errs, errLst...)
				j.DumpedObjects = make(map[string]string)
			}
		}
		if j.DeferredCopyingLevel >= 2 {
			errLst := j.Storages.Delivery(appCtx, j.DumpedObjects)
			errs = append(errs, errLst...)
			j.DumpedObjects = make(map[string]string)
		}
	}

	// cleanup tmp dir
	files, _ := ioutil.ReadDir(tmpDirPath)
	if len(files) == 0 {
		err = os.Remove(tmpDirPath)
		if err != nil {
			errs = append(errs, err)
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
