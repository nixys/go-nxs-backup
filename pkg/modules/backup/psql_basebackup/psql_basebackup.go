package psql_basebackup

import (
	"bytes"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path"
	"regexp"

	appctx "github.com/nixys/nxs-go-appctx/v2"

	"nxs-backup/interfaces"
	"nxs-backup/misc"
	"nxs-backup/modules/backend/exec_cmd"
	"nxs-backup/modules/backend/psql_connect"
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
	backupsList          []string
}

type source struct {
	name      string
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

func Init(jp JobParams) (*job, error) {

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
		dumpedObjects:        make(map[string]string),
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

		j.backupsList = append(j.backupsList, src.Name)
		j.sources = append(j.sources, source{
			name:      src.Name,
			extraKeys: src.ExtraKeys,
			gzip:      src.Gzip,
			connUrl:   connUrl,
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

		tmpBackupFile := misc.GetFileFullPath(tmpDir, src.name, "tar", "", src.gzip)

		if err := createTmpBackup(appCtx, tmpBackupFile, src); err != nil {
			appCtx.Log().Errorf("Failed to create temp backups %s by job %s", tmpBackupFile, j.name)
			errs = append(errs, err...)
			continue
		} else {
			appCtx.Log().Infof("Created temp backups %s by job %s", tmpBackupFile, j.name)
		}

		j.dumpedObjects[src.name] = tmpBackupFile

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

func createTmpBackup(appCtx *appctx.AppContext, tmpBackupFile string, src source) (errs []error) {

	var stderr, stdout bytes.Buffer

	tmpBasebackupPath := path.Join(path.Dir(tmpBackupFile), "pg_basebackup_"+src.name+"_"+misc.GetDateTimeNow(""))
	defer func() { _ = os.RemoveAll(tmpBasebackupPath) }()

	var args []string
	// define command args
	// add extra dump cmd options
	if len(src.extraKeys) > 0 {
		args = append(args, src.extraKeys...)
	}
	// add db connect
	args = append(args, "--dbname="+src.connUrl.String())
	// add data catalog path
	args = append(args, "--pgdata="+tmpBasebackupPath)

	cmd := exec.Command("pg_basebackup", args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		appCtx.Log().Errorf("Unable to start pg_basebackup. Error: %s", err)
		errs = append(errs, err)
		return
	}
	appCtx.Log().Infof("Starting to dump `%s` source", src.name)

	if err := cmd.Wait(); err != nil {
		appCtx.Log().Errorf("Unable to make dump `%s`. Error: %s", src.name, stderr.String())
		errs = append(errs, err)
		return
	}

	stdout.Reset()
	stderr.Reset()

	if err := targz.Archive(tmpBasebackupPath, tmpBackupFile, src.gzip); err != nil {
		appCtx.Log().Errorf("Unable to make tar: %s", err)
		errs = append(errs, err)
		return
	}

	appCtx.Log().Infof("Dumping of source `%s` completed", src.name)

	return
}

func (j *job) Close() error {
	for _, st := range j.storages {
		_ = st.Close()
	}
	return nil
}
