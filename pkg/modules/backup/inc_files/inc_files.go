package inc_files

import (
	"archive/tar"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
	appctx "github.com/nixys/nxs-go-appctx/v2"

	"nxs-backup/interfaces"
	"nxs-backup/misc"
	"nxs-backup/modules/backend/targz"
)

type job struct {
	name                 string
	tmpDir               string
	metadataDir          string
	safetyBackup         bool
	deferredCopyingLevel int
	storages             interfaces.Storages
	targets              map[string]target
	dumpedObjects        map[string]interfaces.DumpObject
}

type target struct {
	path        string
	gzip        bool
	saveAbsPath bool
	excludes    []*regexp.Regexp
}

type metadata map[string]float64

type JobParams struct {
	Name                 string
	TmpDir               string
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

func Init(jp JobParams) (interfaces.Job, error) {

	j := &job{
		name:                 jp.Name,
		tmpDir:               jp.TmpDir,
		safetyBackup:         jp.SafetyBackup,
		deferredCopyingLevel: jp.DeferredCopyingLevel,
		storages:             jp.Storages,
		dumpedObjects:        make(map[string]interfaces.DumpObject),
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
				var excludes []*regexp.Regexp

				excluded := false
				for _, exclPattern := range src.Excludes {
					excl, err := regexp.CompilePOSIX(exclPattern)
					if err != nil {
						return nil, fmt.Errorf("Job `%s` init failed. Unable to process pattern: %s. Error: %s. ", jp.Name, exclPattern, err)
					}
					excludes = append(excludes, excl)

					if excl.MatchString(ofs) {
						excluded = true
					}
				}

				if !excluded {
					ofsPart := src.Name + "/" + misc.GetOfsPart(targetPattern, ofs)
					j.targets[ofsPart] = target{
						path:        ofs,
						gzip:        src.Gzip,
						saveAbsPath: src.SaveAbsPath,
						excludes:    excludes,
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

func (j *job) GetDumpObjects() map[string]interfaces.DumpObject {
	return j.dumpedObjects
}

func (j *job) SetDumpObjectDelivered(ofs string) {
	dumpObj := j.dumpedObjects[ofs]
	dumpObj.Delivered = true
	j.dumpedObjects[ofs] = dumpObj
}

func (j *job) IsBackupSafety() bool {
	return j.safetyBackup
}

func (j *job) DeleteOldBackups(appCtx *appctx.AppContext, ofsPath string) error {
	return j.storages.DeleteOldBackups(appCtx, j, ofsPath)
}

func (j *job) CleanupTmpData(appCtx *appctx.AppContext) error {
	return j.storages.CleanupTmpData(appCtx, j)
}

func (j *job) NeedToMakeBackup() bool {
	return true
}

func (j *job) NeedToUpdateIncMeta() bool {
	return true
}

func (j *job) DoBackup(appCtx *appctx.AppContext, tmpDir string) error {
	var errs *multierror.Error

	for ofsPart, tgt := range j.targets {
		mtd, initMeta, err := j.getMetadata(appCtx, ofsPart)

		if initMeta {
			appCtx.Log().Info("Incremental backup will be reinitialized")

			if err = j.DeleteOldBackups(appCtx, ofsPart); err != nil {
				errs = multierror.Append(errs, err)
			}
		}

		tmpBackupFile := misc.GetFileFullPath(tmpDir, ofsPart, "tar", "", tgt.gzip)
		err = os.MkdirAll(path.Dir(tmpBackupFile), os.ModePerm)
		if err != nil {
			appCtx.Log().Errorf("Job `%s` failed. Unable to create tmp dir with next error: %s", j.name, err)
			errs = multierror.Append(errs, err)
			continue
		}

		if err = j.createTmpBackup(tmpBackupFile, tgt, mtd, initMeta); err != nil {
			appCtx.Log().Errorf("Failed to create temp backups %s by job %s", tmpBackupFile, j.name)
			appCtx.Log().Error(err)
			errs = multierror.Append(errs, err)
			continue
		}

		appCtx.Log().Infof("Created temp backups %s by job %s", tmpBackupFile, j.name)

		j.dumpedObjects[ofsPart] = interfaces.DumpObject{TmpFile: tmpBackupFile}
		if j.deferredCopyingLevel <= 0 {
			err = j.storages.Delivery(appCtx, j)
			if err != nil {
				appCtx.Log().Errorf("Failed to delivery backup by job %s. Errors: %v", j.name, err)
				errs = multierror.Append(errs, err)
			}
		}
	}

	if j.deferredCopyingLevel >= 1 {
		err := j.storages.Delivery(appCtx, j)
		if err != nil {
			appCtx.Log().Errorf("Failed to delivery backup by job %s. Errors: %v", j.name, err)
			errs = multierror.Append(errs, err)
		}
	}

	return errs.ErrorOrNil()
}

func (j *job) getMetadata(appCtx *appctx.AppContext, ofsPart string) (mtd metadata, initMeta bool, err error) {
	var yearMetaFile, monthMetaFile, dayMetaFile io.Reader

	mtd = make(metadata)

	//year := misc.GetDateTimeNow("year")
	moy := misc.GetDateTimeNow("moy")
	dom := misc.GetDateTimeNow("dom")

	initMeta = misc.GetDateTimeNow("doy") == misc.YearlyBackupDay

	yearMetaFile, err = j.getMetadataFile(appCtx, ofsPart, "year.inc")
	if err != nil {
		appCtx.Log().Warnf("Failed to find backup year metadata by job %s. Error: %v", j.name, err)
		initMeta = true
	}

	if !initMeta {
		if !misc.Contains(misc.DecadesBackupDays, dom) {
			dayMetaFile, err = j.getMetadataFile(appCtx, ofsPart, "day.inc")
			if err != nil {
				appCtx.Log().Errorf("Failed to find backup day metadata by job %s", j.name)
				return
			} else {
				mtd, err = j.readMetadata(appCtx, dayMetaFile)
				if err != nil {
					appCtx.Log().Errorf("Failed to read backup day metadata by job %s", j.name)
					return
				}
			}
		} else if moy != "1" {
			monthMetaFile, err = j.getMetadataFile(appCtx, ofsPart, "month.inc")
			if err != nil {
				appCtx.Log().Errorf("Failed to find backup month metadata by job %s", j.name)
				return
			} else {
				mtd, err = j.readMetadata(appCtx, monthMetaFile)
				if err != nil {
					appCtx.Log().Errorf("Failed to read backup month metadata by job %s", j.name)
					return
				}
			}
		} else {
			mtd, err = j.readMetadata(appCtx, yearMetaFile)
			if err != nil {
				appCtx.Log().Errorf("Failed to read backup year metadata by job %s", j.name)
				return
			}
		}
	}
	return
}

// check and get metadata files (include remote storages)
func (j *job) getMetadataFile(appCtx *appctx.AppContext, ofsPart, metadata string) (reader io.Reader, err error) {
	year := misc.GetDateTimeNow("year")

	for i := len(j.storages) - 1; i >= 0; i-- {
		st := j.storages[i]

		reader, err = st.GetFileReader(path.Join(ofsPart, year, "inc_meta_info", metadata))
		if err != nil {
			appCtx.Log().WithField("storage", st.GetName()).Warnf("Unable to get previous metadata '%s' from storage. Error: %s ", metadata, err)
			continue
		}
		break
	}

	if reader == nil {
		err = fs.ErrNotExist
	}

	return
}

// read metadata from file
func (j *job) readMetadata(appCtx *appctx.AppContext, fileReader io.Reader) (mtd metadata, err error) {
	mtd = make(metadata)

	byteValue, err := io.ReadAll(fileReader)
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

func (j *job) createTmpBackup(tmpBackupFile string, tgt target, prevMtd metadata, initMeta bool) error {

	// create new index
	mtd := make(metadata)

	fileWriter, err := targz.GetFileWriter(tmpBackupFile, tgt.gzip)
	if err != nil {
		return err
	}
	defer func() { _ = fileWriter.Close() }()

	tarWriter := tar.NewWriter(fileWriter)
	defer func() { _ = tarWriter.Close() }()

	info, err := os.Stat(tgt.path)
	if err != nil {
		return err
	}

	var baseDir string
	if info.IsDir() {
		baseDir = path.Base(tgt.path)
	}

	headers := map[string]*tar.Header{}

	err = filepath.Walk(tgt.path,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			for _, excl := range tgt.excludes {
				if excl.MatchString(path) {
					return err
				}
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
			header.Format = tar.FormatPAX

			mTime := float64(header.ModTime.UnixNano()) / float64(time.Second)
			aTime := float64(header.AccessTime.UnixNano()) / float64(time.Second)
			cTime := float64(header.ChangeTime.UnixNano()) / float64(time.Second)

			paxRecs := map[string]string{
				"mtime": fmt.Sprintf("%f", mTime),
				"atime": fmt.Sprintf("%f", aTime),
				"ctime": fmt.Sprintf("%f", cTime),
			}

			if info.IsDir() {
				var (
					files   []fs.FileInfo
					dumpDir string
				)
				delimiterSymbol := "\u0000"

				files, err = ioutil.ReadDir(path)
				if err != nil {
					return err
				}
				for _, fi := range files {
					excluded := false
					for _, excl := range tgt.excludes {
						if excl.MatchString(filepath.Join(path, fi.Name())) {
							excluded = true
							break
						}
					}
					if excluded {
						continue
					}

					if fi.IsDir() {
						dumpDir += "D"
					} else if prevMtd[path] == mtd[path] {
						dumpDir += "N"
					} else {
						dumpDir += "Y"
					}
					dumpDir += fi.Name() + delimiterSymbol
				}
				paxRecs["GNU.dumpdir"] = dumpDir + delimiterSymbol
			} else {
				mtd[path] = mTime
			}
			header.PAXRecords = paxRecs

			headers[path] = header

			return err
		})
	if err != nil {
		return err
	}

	for fPath, header := range headers {
		if err = tarWriter.WriteHeader(header); err != nil {
			return err
		}
		if _, ok := header.PAXRecords["GNU.dumpdir"]; !ok {
			func() {
				var file fs.File
				file, err = os.Open(fPath)
				defer func() { _ = file.Close() }()
				if err != nil {
					return
				}
				_, err = io.Copy(tarWriter, file)
				if err != nil {
					return
				}
			}()
		}
	}

	file, err := json.Marshal(mtd)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(path.Join(path.Dir(tmpBackupFile), path.Base(tmpBackupFile)+".inc"), file, 0644)
	if err != nil {
		return err
	}

	if initMeta {
		_, err = os.Create(path.Join(path.Dir(tmpBackupFile), path.Base(tmpBackupFile)+".init"))
	}
	return err
}

func (j *job) Close() error {
	for _, st := range j.storages {
		_ = st.Close()
	}
	return nil
}
