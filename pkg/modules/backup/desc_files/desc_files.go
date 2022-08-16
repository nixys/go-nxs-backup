package desc_files

import (
	"fmt"
	"nxs-backup/modules/backend/targz"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"

	appctx "github.com/nixys/nxs-go-appctx/v2"

	"nxs-backup/interfaces"
	"nxs-backup/misc"
)

type job struct {
	name                 string
	tmpDir               string
	needToMakeBackup     bool
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
	excludes    []*regexp.Regexp
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
		needToMakeBackup:     jp.NeedToMakeBackup,
		safetyBackup:         jp.SafetyBackup,
		deferredCopyingLevel: jp.DeferredCopyingLevel,
		storages:             jp.Storages,
		targets:              make(map[string]target),
		dumpedObjects:        make(map[string]string),
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
	return "desc_files"
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
	return j.needToMakeBackup
}

func (j *job) NeedToUpdateIncMeta() bool {
	return false
}

func (j *job) DoBackup(appCtx *appctx.AppContext, tmpDir string) (errs []error) {

	for ofsPart, tgt := range j.targets {

		tmpBackupFile := misc.GetFileFullPath(tmpDir, ofsPart, "tar", "", tgt.gzip)
		err := os.MkdirAll(path.Dir(tmpBackupFile), os.ModePerm)
		if err != nil {
			appCtx.Log().Errorf("Job `%s` failed. Unable to create tmp dir with next error: %s", j.name, err)
			errs = append(errs, err)
			continue
		}

		if err = targz.Tar(tgt.path, tmpBackupFile, tgt.gzip, tgt.saveAbsPath, tgt.excludes); err != nil {
			appCtx.Log().Errorf("Unable to make tar: %s", err)
			appCtx.Log().Errorf("Failed to create temp backups %s by job %s", tmpBackupFile, j.name)
		} else {
			appCtx.Log().Infof("Created temp backups %s by job %s", tmpBackupFile, j.name)
		}

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

func (j *job) Close() error {
	for _, st := range j.storages {
		_ = st.Close()
	}
	return nil
}
