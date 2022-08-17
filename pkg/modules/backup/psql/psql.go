package psql

import (
	"bytes"
	"fmt"
	"net/url"
	"os/exec"
	"regexp"

	"github.com/jmoiron/sqlx"
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
	sources              []source
	dumpedObjects        map[string]string
	dumpPathsList        []string
}

type source struct {
	name      string
	connect   *sqlx.DB
	connUrl   *url.URL
	targets   []target
	extraKeys []string
	gzip      bool
}

type target struct {
	dbName       string
	ignoreTables []string
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
	TargetDBs     []string
	Excludes      []string
	ExtraKeys     []string
	Gzip          bool
	IsSlave       bool
}

func Init(jp JobParams) (*job, error) {

	// check if mysqldump available
	_, err := exec_cmd.Exec("pg_dump", "--version")
	if err != nil {
		return nil, fmt.Errorf("failed to check pg_dump version. Please check that `pg_dump` installed. Error: %s", err)
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
			if matched, _ := regexp.MatchString(`(-f|--file)`, key); matched {
				return nil, fmt.Errorf("forbidden usage \"--file|-f\" parameter as extra_keys for `postgresql` jobs type")
			}
		}

		dbConn, connUrl, err := psql_connect.GetConnect(src.ConnectParams)
		if err != nil {
			return nil, err
		}

		// fetch all databases
		var databases []string
		err = dbConn.Select(&databases, "SELECT datname FROM pg_database WHERE datistemplate = false;")
		if err != nil {
			return nil, err
		}

		for _, db := range databases {
			if misc.Contains(src.Excludes, db) {
				continue
			}
			var targets []target
			if misc.Contains(src.TargetDBs, "all") || misc.Contains(src.TargetDBs, db) {

				j.dumpPathsList = append(j.dumpPathsList, src.Name+"/"+db)

				var ignoreTables []string
				compRegEx := regexp.MustCompile(`^(?P<db>` + db + `)\.(?P<table>.*$)`)
				for _, excl := range src.Excludes {
					if match := compRegEx.FindStringSubmatch(excl); len(match) > 0 {
						ignoreTables = append(ignoreTables, "--exclude-table="+match[2])
					}
				}
				targets = append(targets, target{
					dbName:       db,
					ignoreTables: ignoreTables,
				})

			}
			j.sources = append(j.sources, source{
				name:      src.Name,
				targets:   targets,
				connect:   dbConn,
				extraKeys: src.ExtraKeys,
				gzip:      src.Gzip,
				connUrl:   connUrl,
			})
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
	return "postgresql"
}

func (j *job) GetTargetOfsList() []string {
	return j.dumpPathsList
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

	for _, src := range j.sources {

		for _, tgt := range src.targets {

			tmpBackupFile := misc.GetFileFullPath(tmpDir, src.name+"_"+tgt.dbName, "sql", "", src.gzip)

			err := createTmpBackup(appCtx, tmpBackupFile, src, tgt)
			if err != nil {
				appCtx.Log().Errorf("Failed to create temp backups %s by job %s", tmpBackupFile, j.name)
				errs = append(errs, err...)
				continue
			} else {
				appCtx.Log().Infof("Created temp backups %s by job %s", tmpBackupFile, j.name)
			}

			j.dumpedObjects[src.name+"/"+tgt.dbName] = tmpBackupFile

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

func createTmpBackup(appCtx *appctx.AppContext, tmpBackupPath string, src source, target target) (errs []error) {

	backupWriter, err := targz.GetFileWriter(tmpBackupPath, src.gzip)
	if err != nil {
		appCtx.Log().Errorf("Unable to create tmp file. Error: %s", err)
		return append(errs, err)
	}
	defer func() { _ = backupWriter.Close }()

	var args []string
	// define command args
	// add tables exclude
	if len(target.ignoreTables) > 0 {
		args = append(args, target.ignoreTables...)
	}
	// add extra dump cmd options
	if len(src.extraKeys) > 0 {
		args = append(args, src.extraKeys...)
	}
	// add db name
	src.connUrl.Path = target.dbName
	args = append(args, "--dbname="+src.connUrl.String())

	var stderr bytes.Buffer
	cmd := exec.Command("pg_dump", args...)
	cmd.Stdout = backupWriter
	cmd.Stderr = &stderr

	if err = cmd.Start(); err != nil {
		appCtx.Log().Errorf("Unable to start pd_dump. Error: %s", err)
		errs = append(errs, err)
		return
	}
	appCtx.Log().Infof("Starting a `%s` dump", target.dbName)

	if err = cmd.Wait(); err != nil {
		appCtx.Log().Errorf("Unable to dump `%s`. Error: %s", target.dbName, stderr.String())
		errs = append(errs, err)
		return
	}

	appCtx.Log().Infof("Dump of `%s` completed", target.dbName)

	return
}

func (j *job) Close() error {
	for _, src := range j.sources {
		_ = src.connect.Close()
	}
	for _, st := range j.storages {
		_ = st.Close()
	}
	return nil
}
