package inc_files

import (
	"archive/tar"
	"encoding/json"
	"errors"
	"fmt"
	appctx "github.com/nixys/nxs-go-appctx/v2"
	"io"
	"io/fs"
	"io/ioutil"
	"nxs-backup/interfaces"
	"nxs-backup/misc"
	"nxs-backup/modules/backend/targz"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

type job struct {
	name                 string
	tmpDir               string
	metadataDir          string
	safetyBackup         bool
	deferredCopyingLevel int
	storages             interfaces.Storages
	targets              map[string]target
	dumpedObjects        map[string]string
}

type target struct {
	path        string
	gzip        bool
	saveAbsPath bool
}

type metadata map[string]float64

type JobParams struct {
	Name                 string
	TmpDir               string
	MetadataDir          string
	SafetyBackup         bool
	DeferredCopyingLevel int
	Storages             interfaces.Storages
	Sources              []SourceParams
}

type SourceParams struct {
	Name        string
	Targets     []string
	Excludes    []string
	Gzip        bool
	SaveAbsPath bool
}

func Init(jp JobParams) (*job, error) {

	j := &job{
		name:                 jp.Name,
		tmpDir:               jp.TmpDir,
		safetyBackup:         jp.SafetyBackup,
		deferredCopyingLevel: jp.DeferredCopyingLevel,
		storages:             jp.Storages,
		dumpedObjects:        make(map[string]string),
		targets:              make(map[string]target),
	}

	for _, src := range jp.Sources {

		for _, targetPattern := range src.Targets {

			for strings.HasSuffix(targetPattern, "/") {
				targetPattern = strings.TrimSuffix(targetPattern, "/")
			}

			targetOfsList, err := filepath.Glob(targetPattern)
			if err != nil {
				return nil, fmt.Errorf("Job `%s` init failed. Unable to process pattern: %s. Error: %s. ", jp.Name, targetPattern, err)
			}

			for _, ofs := range targetOfsList {

				excluded := false
				for _, exclPattern := range src.Excludes {

					match, err := filepath.Match(exclPattern, ofs)
					if err != nil {
						return nil, fmt.Errorf("Job `%s` init failed. Unable to process pattern: %s. Error: %s. ", jp.Name, exclPattern, err)
					}
					if match {
						excluded = true
						break
					}
				}

				if !excluded {
					ofsPart := src.Name + "/" + misc.GetOfsPart(targetPattern, ofs)
					j.targets[ofsPart] = target{
						path:        ofs,
						gzip:        src.Gzip,
						saveAbsPath: src.SaveAbsPath,
					}
				}
			}
		}
	}

	return j, nil
}

func (j *job) GetName() string {
	return j.name
}

func (j *job) GetTempDir() string {
	return j.tmpDir
}

func (j *job) GetType() string {
	return "inc_files"
}

func (j *job) GetTargetOfsList() (ofsList []string) {
	for ofs := range j.targets {
		ofsList = append(ofsList, ofs)
	}
	return
}

func (j *job) GetStoragesCount() int {
	return len(j.storages)
}

func (j *job) GetDumpedObjects() map[string]string {
	return j.dumpedObjects
}

func (j *job) IsBackupSafety() bool {
	return j.safetyBackup
}

func (j *job) DeleteOldBackups(appCtx *appctx.AppContext) []error {
	return j.storages.DeleteOldBackups(appCtx, j)
}

func (j *job) NeedToMakeBackup() bool {
	return true
}

func (j *job) NeedToUpdateIncMeta() bool {
	return true
}

func (j *job) DoBackup(appCtx *appctx.AppContext, tmpDir string) (errs []error) {

	//year := misc.GetDateTimeNow("year")
	moy := misc.GetDateTimeNow("moy")
	dom := misc.GetDateTimeNow("dom")

	for ofsPart, tgt := range j.targets {
		var (
			err                                      error
			getErrs                                  []error
			initMeta                                 bool
			yearMetaFile, monthMetaFile, dayMetaFile fs.File
		)
		mtd := make(metadata)

		yearMetaFile, err, getErrs = j.getMetadataFile(ofsPart, "year.inc")
		if len(getErrs) > 0 {
			appCtx.Log().Errorf("Unable to get matadata from storages by job %s. Errors: %v", j.name, getErrs)
		}
		if err != nil {
			appCtx.Log().Errorf("Failed to find backup year metadata by job %s. Error: %v", j.name, err)
			appCtx.Log().Info("Incremental backup will be reinitialized")
			initMeta = true
			// TODO Add backup dir full cleanup
		} else {
			if !misc.Contains(misc.DecadesBackupDays, dom) {
				dayMetaFile, err, getErrs = j.getMetadataFile(ofsPart, "day.inc")
				if len(getErrs) > 0 {
					appCtx.Log().Errorf("Unable to get matadata from storages by job %s. Errors: %v", j.name, getErrs)
				}
				if err != nil {
					appCtx.Log().Errorf("Failed to find backup day metadata by job %s", j.name)
					errs = append(errs, err)
					continue
				} else {
					mtd, err = j.readMetadata(appCtx, dayMetaFile)
					if err != nil {
						appCtx.Log().Errorf("Failed to read backup day metadata by job %s", j.name)
						errs = append(errs, err)
						continue
					}
				}
			} else if moy != "1" {
				monthMetaFile, err, getErrs = j.getMetadataFile(ofsPart, "month.inc")
				if len(getErrs) > 0 {
					appCtx.Log().Errorf("Unable to get matadata from storages by job %s. Errors: %v", j.name, getErrs)
				}
				if err != nil {
					appCtx.Log().Errorf("Failed to find backup month metadata by job %s", j.name)
					errs = append(errs, err)
					continue
				} else {
					mtd, err = j.readMetadata(appCtx, monthMetaFile)
					if err != nil {
						appCtx.Log().Errorf("Failed to read backup month metadata by job %s", j.name)
						errs = append(errs, err)
						continue
					}
				}
			} else {
				mtd, err = j.readMetadata(appCtx, yearMetaFile)
				if err != nil {
					appCtx.Log().Errorf("Failed to read backup year metadata  by job %s", j.name)
					errs = append(errs, err)
					continue
				}
			}
		}

		tmpBackupFile := misc.GetFileFullPath(tmpDir, ofsPart, "tar", "", tgt.gzip)
		err = os.MkdirAll(path.Dir(tmpBackupFile), os.ModePerm)
		if err != nil {
			appCtx.Log().Errorf("Job `%s` failed. Unable to create tmp dir with next error: %s", j.name, err)
			errs = append(errs, err)
			continue
		}

		if err = j.createTmpBackup(appCtx, tmpBackupFile, tgt, mtd, initMeta); err != nil {
			appCtx.Log().Errorf("Failed to create temp backups %s by job %s", tmpBackupFile, j.name)
			appCtx.Log().Error(err)
			errs = append(errs, err)
			continue
		}

		appCtx.Log().Infof("Created temp backups %s by job %s", tmpBackupFile, j.name)

		j.dumpedObjects[ofsPart] = tmpBackupFile
		if j.deferredCopyingLevel <= 0 {
			errLst := j.storages.Delivery(appCtx, j)
			if len(errLst) > 0 {
				appCtx.Log().Errorf("Failed to delivery backup by job %s", j.name)
				appCtx.Log().Error(errLst)
				errs = append(errs, errLst...)
			}
			j.dumpedObjects = make(map[string]string)
		}
	}

	if j.deferredCopyingLevel >= 1 {
		errLst := j.storages.Delivery(appCtx, j)
		if len(errLst) > 0 {
			appCtx.Log().Errorf("Failed to delivery backup by job %s", j.name)
			appCtx.Log().Error(errLst)
			errs = append(errs, errLst...)
		}
	}

	return
}

func (j *job) createTmpBackup(appCtx *appctx.AppContext, tmpBackupFile string, tgt target, prevMtd metadata, initMeta bool) (err error) {

	// create new index
	mtd := make(metadata)

	fileWriter, err := targz.GetFileWriter(tmpBackupFile, tgt.gzip)
	if err != nil {
		return
	}
	defer func() { _ = fileWriter.Close() }()

	tarWriter := tar.NewWriter(fileWriter)
	defer func() { _ = tarWriter.Close() }()

	info, err := os.Stat(tgt.path)
	if err != nil {
		return
	}

	var baseDir string
	if info.IsDir() {
		baseDir = filepath.Base(tgt.path)
	}

	err = filepath.Walk(tgt.path,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			header, err := tar.FileInfoHeader(info, info.Name())
			if err != nil {
				return err
			}

			if tgt.saveAbsPath {
				header.Name = path
			} else if baseDir != "" {
				header.Name = filepath.Join(baseDir, strings.TrimPrefix(path, tgt.path))
			}

			if err = tarWriter.WriteHeader(header); err != nil {
				return err
			}

			if info.IsDir() {
				return nil
			}

			mtd[path] = float64(info.ModTime().UnixNano()) / float64(time.Second)

			if prevMtd[path] != mtd[path] {
				file, err := os.Open(path)
				if err != nil {
					return err
				}
				defer func() { _ = file.Close() }()

				_, err = io.Copy(tarWriter, file)
			}

			return err
		})
	if err != nil {
		return
	}

	file, err := json.Marshal(mtd)
	if err != nil {
		return
	}
	err = ioutil.WriteFile(path.Join(path.Dir(tmpBackupFile), path.Base(tmpBackupFile)+".inc"), file, 0644)
	if err != nil {
		return
	}

	if initMeta {
		_, err = os.Create(path.Join(path.Dir(tmpBackupFile), path.Base(tmpBackupFile)+"init"))
	}
	return
}

// check and get metadata files (include remote storages)
func (j *job) getMetadataFile(ofsPart, metadata string) (file fs.File, err error, getErrs []error) {

	year := misc.GetDateTimeNow("year")

	for i := len(j.storages) - 1; i == 0; i-- {
		st := j.storages[i]

		file, err = st.GetFile(path.Join(ofsPart, year, "inc_meta_info", metadata))
		if err != nil {
			// TODO Add storage name to err
			if !errors.Is(err, fs.ErrNotExist) {
				getErrs = append(getErrs, fmt.Errorf("Unable to get previous metadata from storage. Error: %s ", err))
			}
			continue
		}
		break
	}

	if file == nil {
		err = fs.ErrNotExist
		return
	}

	_, err = file.Stat()
	return
}

// read metadata from file
func (j *job) readMetadata(appCtx *appctx.AppContext, file fs.File) (mtd metadata, err error) {
	mtd = make(metadata)

	byteValue, err := io.ReadAll(file)
	if err != nil {
		appCtx.Log().Errorf("Failed to read metadata file. Error: %s", err)
		return
	}

	err = json.Unmarshal(byteValue, &mtd)
	if err != nil {
		appCtx.Log().Errorf("Failed to parse metadata file. Error: %s", err)
	}

	return
}

func (j *job) Close() error {
	for _, st := range j.storages {
		_ = st.Close()
	}
	return nil
}
