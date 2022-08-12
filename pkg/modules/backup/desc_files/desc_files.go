package desc_files

import (
	"fmt"
	"path/filepath"
	"strings"

	appctx "github.com/nixys/nxs-go-appctx/v2"

	"nxs-backup/interfaces"
	"nxs-backup/misc"
	"nxs-backup/modules/backend/targz"
)

type job struct {
	name                 string
	tmpDir               string
	needToMakeBackup     bool
	safetyBackup         bool
	deferredCopyingLevel int
	storages             interfaces.Storages
	sources              []source
	dumpedObjects        map[string]string
	dumpPathsList        []string
}

type source struct {
	name        string
	targets     []map[string]string
	gzip        bool
	saveAbsPath bool
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
		dumpedObjects:        make(map[string]string),
	}

	for _, src := range jp.Sources {

		var targets []map[string]string
		for _, targetPattern := range src.Targets {

			for strings.HasSuffix(targetPattern, "/") {
				targetPattern = strings.TrimSuffix(targetPattern, "/")
			}

			targetOfsList, err := filepath.Glob(targetPattern)
			if err != nil {
				return nil, fmt.Errorf("Job `%s` init failed. Unable to process pattern: %s. Error: %s. ", jp.Name, targetPattern, err)
			}

			targetOfsMap := make(map[string]string)
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
					ofsPart := misc.GetOfsPart(targetPattern, ofs)
					targetOfsMap[ofsPart] = ofs
					j.dumpPathsList = append(j.dumpPathsList, src.Name+"/"+ofsPart)
				}
			}

			targets = append(targets, targetOfsMap)
		}

		j.sources = append(j.sources, source{
			name:        src.Name,
			targets:     targets,
			gzip:        src.Gzip,
			saveAbsPath: src.SaveAbsPath,
		})
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

func (j *job) GetTargetOfsList() []string {
	return j.dumpPathsList
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

	for _, src := range j.sources {

		for _, target := range src.targets {

			for ofsPart, ofs := range target {

				tmpBackupFile := misc.GetFileFullPath(tmpDir, src.name+"_"+ofsPart, "tar", "", src.gzip)

				if err := createTmpBackup(appCtx, tmpBackupFile, ofs, src.gzip); err != nil {
					appCtx.Log().Errorf("Failed to create temp backups %s by job %s", tmpBackupFile, j.name)
					errs = append(errs, err)
					continue
				} else {
					appCtx.Log().Infof("Created temp backups %s by job %s", tmpBackupFile, j.name)
				}

				j.dumpedObjects[src.name+"/"+ofsPart] = tmpBackupFile
			}

			if j.deferredCopyingLevel <= 0 {
				errLst := j.storages.Delivery(appCtx, j)
				errs = append(errs, errLst...)
				j.dumpedObjects = make(map[string]string)
			}
		}

		if j.deferredCopyingLevel == 1 {
			errLst := j.storages.Delivery(appCtx, j)
			errs = append(errs, errLst...)
			j.dumpedObjects = make(map[string]string)
		}
	}

	if j.deferredCopyingLevel >= 2 {
		errLst := j.storages.Delivery(appCtx, j)
		errs = append(errs, errLst...)
	}

	return
}

func createTmpBackup(appCtx *appctx.AppContext, tmpBackupPath, ofs string, gZip bool) (err error) {
	if err := targz.Tar(ofs, tmpBackupPath, gZip, true); err != nil {
		appCtx.Log().Errorf("Unable to make tar: %s", err)
	}
	return
}

func (j *job) Close() error {
	for _, st := range j.storages {
		_ = st.Close()
	}
	return nil
}
