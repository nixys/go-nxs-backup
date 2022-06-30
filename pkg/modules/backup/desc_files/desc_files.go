package desc_files

import (
	"archive/tar"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	appctx "github.com/nixys/nxs-go-appctx/v2"

	"nxs-backup/interfaces"
	"nxs-backup/misc"
)

type descFileJob struct {
	name                 string
	tmpDir               string
	needToMakeBackup     bool
	safetyBackup         bool
	deferredCopyingLevel int
	storages             interfaces.Storages
	sources              []source
	dumpedObjects        map[string]string
	ofsPartsList         []string
}

type source struct {
	targets []map[string]string
	gzip    bool
}

type JobParams struct {
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

func Init(p JobParams) (*descFileJob, error) {

	job := &descFileJob{
		name:                 p.Name,
		tmpDir:               p.TmpDir,
		needToMakeBackup:     p.NeedToMakeBackup,
		safetyBackup:         p.SafetyBackup,
		deferredCopyingLevel: p.DeferredCopyingLevel,
		storages:             p.Storages,
		dumpedObjects:        make(map[string]string),
	}

	for _, s := range p.Sources {

		var targets []map[string]string
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
					job.ofsPartsList = append(job.ofsPartsList, ofsPart)
				}
			}

			targets = append(targets, targetOfsMap)
		}

		job.sources = append(job.sources, source{
			targets: targets,
			gzip:    s.Gzip,
		})
	}

	return job, nil
}

func (j *descFileJob) GetName() string {
	return j.name
}

func (j *descFileJob) GetTempDir() string {
	return j.tmpDir
}

func (j *descFileJob) GetType() string {
	return "files"
}

func (j *descFileJob) IsBackupSafety() bool {
	return j.safetyBackup
}

func (j *descFileJob) CleanupOldBackups(appCtx *appctx.AppContext) []error {
	return j.storages.CleanupOldBackups(appCtx, j.ofsPartsList)
}

func (j *descFileJob) IsNeedToMakeBackup() bool {
	return j.needToMakeBackup
}

func (j *descFileJob) DoBackup(appCtx *appctx.AppContext, tmpDir string) (errs []error) {

	for _, src := range j.sources {

		for _, target := range src.targets {

			for ofsPart, ofs := range target {

				tmpBackupFullPath := misc.GetBackupFullPath(tmpDir, ofsPart, "tar", "", src.gzip)
				err := createTmpBackup(appCtx, tmpBackupFullPath, ofs, src.gzip)
				if err != nil {
					appCtx.Log().Errorf("Failed to create temp backups %s by job %s", tmpBackupFullPath, j.name)
					errs = append(errs, err)
					continue
				} else {
					appCtx.Log().Infof("Created temp backups %s by job %s", tmpBackupFullPath, j.name)
				}

				j.dumpedObjects[ofsPart] = tmpBackupFullPath
			}

			if j.deferredCopyingLevel <= 0 {
				errLst := j.storages.Delivery(appCtx, j.dumpedObjects)
				errs = append(errs, errLst...)
				j.dumpedObjects = make(map[string]string)
			}
		}

		if j.deferredCopyingLevel == 1 {
			errLst := j.storages.Delivery(appCtx, j.dumpedObjects)
			errs = append(errs, errLst...)
			j.dumpedObjects = make(map[string]string)
		}
	}

	if j.deferredCopyingLevel >= 2 {
		errLst := j.storages.Delivery(appCtx, j.dumpedObjects)
		errs = append(errs, errLst...)
	}

	return
}

func createTmpBackup(appCtx *appctx.AppContext, tmpBackupPath, ofs string, gZip bool) (err error) {
	backupWriter, err := misc.GetBackupWriter(tmpBackupPath, gZip)
	defer backupWriter.Close()
	if err != nil {
		appCtx.Log().Errorf("Unable to create tmp file: %s", err)
		return err
	}

	tarWriter := tar.NewWriter(backupWriter)
	defer tarWriter.Close()

	err = tarDirectory(ofs, tarWriter, filepath.Dir(ofs))
	if err != nil {
		appCtx.Log().Errorf("Unable to make tar: %s", err)
	}
	return
}

func tarDirectory(directory string, tarWriter *tar.Writer, subPath string) error {

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
			err := tarDirectory(currentPath, tarWriter, subPath)
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
