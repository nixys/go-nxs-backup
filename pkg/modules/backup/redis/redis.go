package redis

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/hashicorp/go-multierror"

	"nxs-backup/interfaces"
	"nxs-backup/misc"
	"nxs-backup/modules/backend/cmd"
	"nxs-backup/modules/backend/targz"
	"nxs-backup/modules/connectors/redis_connect"
	"nxs-backup/modules/logger"
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

func Init(jp JobParams) (interfaces.Job, error) {

	// check if redis-cli available
	_, err := cmd.Exec("redis-cli", "--version")
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

func (j *job) DeleteOldBackups(logCh chan logger.LogRecord, ofsPath string) error {
	return j.storages.DeleteOldBackups(logCh, j, ofsPath)
}

func (j *job) CleanupTmpData() error {
	return j.storages.CleanupTmpData(j)
}

func (j *job) DoBackup(logCh chan logger.LogRecord, tmpDir string) error {
	var errs *multierror.Error

	for ofsPart, tgt := range j.targets {

		if err := j.createTmpBackup(logCh, tmpDir, ofsPart, tgt); err != nil {
			logCh <- logger.Log(j.name, "").Error("Failed to create temp backup.")
			errs = multierror.Append(errs, err)
			continue
		}

		if j.deferredCopyingLevel <= 0 {
			err := j.storages.Delivery(logCh, j)
			errs = multierror.Append(errs, err)
		}
	}

	if j.deferredCopyingLevel >= 1 {
		err := j.storages.Delivery(logCh, j)
		errs = multierror.Append(errs, err)
	}

	return errs.ErrorOrNil()
}

func (j *job) createTmpBackup(logCh chan logger.LogRecord, tmpDir, tgtName string, tgt target) error {

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
		logCh <- logger.Log(j.name, "").Errorf("Unable to start redis-cli. Error: %s", err)
		return err
	}
	logCh <- logger.Log(j.name, "").Infof("Starting to dump `%s` source", tgtName)

	if err := cmd.Wait(); err != nil {
		logCh <- logger.Log(j.name, "").Errorf("Unable to make dump `%s`. Error: %s", tgtName, stderr.String())
		return err
	}

	if tgt.gzip {
		if err := targz.GZip(tmpBackupRdb, tmpBackupFile); err != nil {
			logCh <- logger.Log(j.name, "").Errorf("Unable to archivate tmp backup: %s", err)
			return err
		}
		_ = os.RemoveAll(tmpBackupRdb)
	}

	logCh <- logger.Log(j.name, "").Infof("Dumping of source `%s` completed", tgtName)
	logCh <- logger.Log(j.name, "").Debugf("Created temp backup %s", tmpBackupFile)

	j.dumpedObjects[tgtName] = interfaces.DumpObject{TmpFile: tmpBackupFile}

	return nil
}

func (j *job) Close() error {
	for _, st := range j.storages {
		_ = st.Close()
	}
	return nil
}
