package psql_basebackup

import (
	"bytes"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path"
	"regexp"

	"github.com/hashicorp/go-multierror"
	appctx "github.com/nixys/nxs-go-appctx/v2"

	"nxs-backup/interfaces"
	"nxs-backup/misc"
	"nxs-backup/modules/backend/exec_cmd"
	"nxs-backup/modules/backend/targz"
	"nxs-backup/modules/connectors/psql_connect"
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
	connUrl   *url.URL
	extraKeys []string
	gzip      bool
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
	ConnectParams psql_connect.Params
	ExtraKeys     []string
	Gzip          bool
	IsSlave       bool
}

func Init(jp JobParams) (interfaces.Job, error) {

	// check if mysqldump available
	_, err := exec_cmd.Exec("pg_basebackup", "--version")
	if err != nil {
		return nil, fmt.Errorf("Job `%s` init failed. Failed to check pg_basebackup version. Please check that `pg_dump` installed. Error: %s ", jp.Name, err)
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

		for _, key := range src.ExtraKeys {
			if matched, _ := regexp.MatchString(`(-D|--pgdata=)`, key); matched {
				return nil, fmt.Errorf("Job `%s` init failed. Forbidden usage \"--pgdata|-D\" parameter as extra_keys for `postgresql_basebackup` jobs type ", jp.Name)
			}
		}

		conn, connUrl, err := psql_connect.GetConnect(src.ConnectParams)
		if err != nil {
			return nil, fmt.Errorf("Job `%s` init failed. PSQL connect error: %s ", jp.Name, err)
		}
		_ = conn.Close()

		j.targets[src.Name] = target{
			extraKeys: src.ExtraKeys,
			gzip:      src.Gzip,
			connUrl:   connUrl,
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
	return "postgresql_basebackup"
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

func (j *job) DeleteOldBackups(appCtx *appctx.AppContext, ofsPath string) error {
	return j.storages.DeleteOldBackups(appCtx, j, ofsPath)
}

func (j *job) CleanupTmpData(appCtx *appctx.AppContext) error {
	return j.storages.CleanupTmpData(appCtx, j)
}

func (j *job) DoBackup(appCtx *appctx.AppContext, tmpDir string) error {
	var errs *multierror.Error

	for ofsPart, tgt := range j.targets {

		tmpBackupFile := misc.GetFileFullPath(tmpDir, ofsPart, "tar", "", tgt.gzip)

		if err := createTmpBackup(appCtx, tmpBackupFile, ofsPart, tgt); err != nil {
			appCtx.Log().Errorf("Failed to create temp backups %s by job %s", tmpBackupFile, j.name)
			errs = multierror.Append(errs, err)
			continue
		} else {
			appCtx.Log().Infof("Created temp backups %s by job %s", tmpBackupFile, j.name)
		}

		j.dumpedObjects[ofsPart] = interfaces.DumpObject{TmpFile: tmpBackupFile}

		if j.deferredCopyingLevel <= 0 {
			err := j.storages.Delivery(appCtx, j)
			errs = multierror.Append(errs, err)
		}
	}

	if j.deferredCopyingLevel >= 1 {
		err := j.storages.Delivery(appCtx, j)
		errs = multierror.Append(errs, err)
	}

	return errs.ErrorOrNil()
}

func createTmpBackup(appCtx *appctx.AppContext, tmpBackupFile, tgtName string, tgt target) error {

	var stderr, stdout bytes.Buffer

	tmpBasebackupPath := path.Join(path.Dir(tmpBackupFile), "pg_basebackup_"+tgtName+"_"+misc.GetDateTimeNow(""))

	var args []string
	// define command args
	// add extra dump cmd options
	if len(tgt.extraKeys) > 0 {
		args = append(args, tgt.extraKeys...)
	}
	// add db connect
	args = append(args, "--dbname="+tgt.connUrl.String())
	// add data catalog path
	args = append(args, "--pgdata="+tmpBasebackupPath)

	cmd := exec.Command("pg_basebackup", args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		appCtx.Log().Errorf("Unable to start pg_basebackup. Error: %s", err)
		return err
	}
	appCtx.Log().Infof("Starting to dump `%s` source", tgtName)

	if err := cmd.Wait(); err != nil {
		appCtx.Log().Errorf("Unable to make dump `%s`. Error: %s", tgtName, stderr.String())
		return err
	}

	if err := targz.Tar(tmpBasebackupPath, tmpBackupFile, tgt.gzip, false, nil); err != nil {
		appCtx.Log().Errorf("Unable to make tar: %s", err)
		return err
	}
	_ = os.RemoveAll(tmpBasebackupPath)

	appCtx.Log().Infof("Dumping of source `%s` completed", tgtName)

	return nil
}

func (j *job) Close() error {
	for _, st := range j.storages {
		_ = st.Close()
	}
	return nil
}
