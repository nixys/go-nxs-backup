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
	sources              []source
	dumpedObjects        map[string]string
	backupsList          []string
}

type source struct {
	name string
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
		dumpedObjects:        make(map[string]string),
	}

	for _, src := range jp.Sources {

		conn, dsn, err := redis_connect.GetConnectAndDSN(src.ConnectParams)
		if err != nil {
			return nil, fmt.Errorf("Job `%s` init failed. Redis connect error: %s ", jp.Name, err)
		}
		_ = conn.Close()

		j.backupsList = append(j.backupsList, src.Name)
		j.sources = append(j.sources, source{
			name: src.Name,
			gzip: src.Gzip,
			dsn:  dsn,
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
	return "databases"
}

func (j *job) IsBackupSafety() bool {
	return j.safetyBackup
}

func (j *job) IsNeedToMakeBackup() bool {
	return j.needToMakeBackup
}

func (j *job) CleanupOldBackups(appCtx *appctx.AppContext) []error {
	return j.storages.CleanupOldBackups(appCtx, j.backupsList)
}

func (j *job) DoBackup(appCtx *appctx.AppContext, tmpDir string) (errs []error) {

	for _, src := range j.sources {

		if err := j.createTmpBackup(appCtx, tmpDir, src); err != nil {
			appCtx.Log().Errorf("Failed to create temp backup by job %s", j.name)
			errs = append(errs, err...)
			continue
		}

		if j.deferredCopyingLevel <= 0 {
			errLst := j.storages.Delivery(appCtx, j.dumpedObjects)
			errs = append(errs, errLst...)
			j.dumpedObjects = make(map[string]string)
		}
	}

	if j.deferredCopyingLevel >= 1 {
		errLst := j.storages.Delivery(appCtx, j.dumpedObjects)
		errs = append(errs, errLst...)
	}

	return
}

func (j *job) createTmpBackup(appCtx *appctx.AppContext, tmpDir string, src source) (errs []error) {

	var stderr, stdout bytes.Buffer

	tmpBackupFile := misc.GetFileFullPath(tmpDir, src.name, "rdb", "", src.gzip)

	tmpBackupRdb := strings.TrimSuffix(tmpBackupFile, ".gz")

	var args []string
	// define command args
	// add db connect
	args = append(args, "-u", src.dsn)
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
	appCtx.Log().Infof("Starting to dump `%s` source", src.name)

	if err := cmd.Wait(); err != nil {
		appCtx.Log().Errorf("Unable to make dump `%s`. Error: %s", src.name, stderr.String())
		errs = append(errs, err)
		return
	}

	if src.gzip {
		if err := targz.GZip(tmpBackupRdb, tmpBackupFile); err != nil {
			appCtx.Log().Errorf("Unable to archivate tmp backup: %s", err)
			errs = append(errs, err)
			return
		}
		_ = os.RemoveAll(tmpBackupRdb)
	}

	appCtx.Log().Infof("Dumping of source `%s` completed", src.name)
	appCtx.Log().Infof("Created temp backup %s by job %s", tmpBackupFile, j.name)

	j.dumpedObjects[src.name] = tmpBackupFile

	return
}

func (j *job) Close() error {
	for _, st := range j.storages {
		_ = st.Close()
	}
	return nil
}
