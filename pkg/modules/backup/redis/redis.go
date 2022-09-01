package redis

import (
	"bytes"
	"fmt"
	appctx "github.com/nixys/nxs-go-appctx/v2"
	"nxs-backup/modules/backend/targz"
	"nxs-backup/modules/connectors/redis_connect"
	"os"
	"os/exec"
	"strings"

	"nxs-backup/interfaces"
	"nxs-backup/misc"
	"nxs-backup/modules/backend/exec_cmd"
)

type job struct {
	name                 string
	tmpDir               string
	needToMakeBackup     bool
	safetyBackup         bool
	deferredCopyingLevel int
	storages             interfaces.Storages
	targets              map[string]target
	dumpedObjects        map[string]interfaces.DumpObject
}

type target struct {
	dsn  string
	gzip bool
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
	Name          string
	ConnectParams redis_connect.Params
	Gzip          bool
}

func Init(jp JobParams) (*job, error) {

	// check if redis-cli available
	_, err := exec_cmd.Exec("redis-cli", "--version")
	if err != nil {
		return nil, fmt.Errorf("Job `%s` init failed. Failed to check redis-cli version. Please check that `redis-cli` installed. Error: %s ", jp.Name, err)
	}

	j := &job{
		name:                 jp.Name,
		tmpDir:               jp.TmpDir,
		needToMakeBackup:     jp.NeedToMakeBackup,
		safetyBackup:         jp.SafetyBackup,
		deferredCopyingLevel: jp.DeferredCopyingLevel,
		storages:             jp.Storages,
		targets:              make(map[string]target),
		dumpedObjects:        make(map[string]interfaces.DumpObject),
	}

	for _, src := range jp.Sources {

		conn, dsn, err := redis_connect.GetConnectAndDSN(src.ConnectParams)
		if err != nil {
			return nil, fmt.Errorf("Job `%s` init failed. Redis connect error: %s ", jp.Name, err)
		}
		_ = conn.Close()

		j.targets[src.Name] = target{
			gzip: src.Gzip,
			dsn:  dsn,
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
	return "redis"
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

func (j *job) NeedToMakeBackup() bool {
	return j.needToMakeBackup
}

func (j *job) NeedToUpdateIncMeta() bool {
	return false
}

func (j *job) DeleteOldBackups(appCtx *appctx.AppContext) []error {
	return j.storages.DeleteOldBackups(appCtx, j)
}

func (j *job) CleanupTmpData(appCtx *appctx.AppContext) error {
	return j.storages.CleanupTmpData(appCtx, j)
}

func (j *job) DoBackup(appCtx *appctx.AppContext, tmpDir string) (errs []error) {

	for ofsPart, tgt := range j.targets {

		if errList := j.createTmpBackup(appCtx, tmpDir, ofsPart, tgt); errList != nil {
			appCtx.Log().Errorf("Failed to create temp backup by job %s", j.name)
			errs = append(errs, errList...)
			continue
		}

		if j.deferredCopyingLevel <= 0 {
			err := j.storages.Delivery(appCtx, j)
			errs = append(errs, err)
		}
	}

	if j.deferredCopyingLevel >= 1 {
		err := j.storages.Delivery(appCtx, j)
		errs = append(errs, err)
	}

	return
}

func (j *job) createTmpBackup(appCtx *appctx.AppContext, tmpDir, tgtName string, tgt target) (errs []error) {

	var stderr, stdout bytes.Buffer

	tmpBackupFile := misc.GetFileFullPath(tmpDir, tgtName, "rdb", "", tgt.gzip)

	tmpBackupRdb := strings.TrimSuffix(tmpBackupFile, ".gz")

	var args []string
	// define command args
	// add db connect
	args = append(args, "-u", tgt.dsn)
	// add data catalog path
	args = append(args, "--rdb", tmpBackupRdb)

	cmd := exec.Command("redis-cli", args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		appCtx.Log().Errorf("Unable to start redis-cli. Error: %s", err)
		errs = append(errs, err)
		return
	}
	appCtx.Log().Infof("Starting to dump `%s` source", tgtName)

	if err := cmd.Wait(); err != nil {
		appCtx.Log().Errorf("Unable to make dump `%s`. Error: %s", tgtName, stderr.String())
		errs = append(errs, err)
		return
	}

	if tgt.gzip {
		if err := targz.GZip(tmpBackupRdb, tmpBackupFile); err != nil {
			appCtx.Log().Errorf("Unable to archivate tmp backup: %s", err)
			errs = append(errs, err)
			return
		}
		_ = os.RemoveAll(tmpBackupRdb)
	}

	appCtx.Log().Infof("Dumping of source `%s` completed", tgtName)
	appCtx.Log().Infof("Created temp backup %s by job %s", tmpBackupFile, j.name)

	j.dumpedObjects[tgtName] = interfaces.DumpObject{TmpFile: tmpBackupFile}

	return
}

func (j *job) Close() error {
	for _, st := range j.storages {
		_ = st.Close()
	}
	return nil
}
