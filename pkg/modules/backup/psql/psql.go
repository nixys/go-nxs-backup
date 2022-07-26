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
	"nxs-backup/modules/backend/psql_connect"
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
	databasesList        []string
}

type source struct {
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
	ConnectParams psql_connect.Params
	TargetDBs     []string
	Excludes      []string
	ExtraKeys     []string
	Gzip          bool
	IsSlave       bool
}

func Init(params JobParams) (*job, error) {

	// check if mysqldump available
	_, err := exec_cmd.Exec("pg_dump", "--version")
	if err != nil {
		return nil, fmt.Errorf("failed to check pg_dump version. Please check that `pg_dump` installed. Error: %s", err)
	}

	job := &job{
		name:                 params.Name,
		tmpDir:               params.TmpDir,
		needToMakeBackup:     params.NeedToMakeBackup,
		safetyBackup:         params.SafetyBackup,
		deferredCopyingLevel: params.DeferredCopyingLevel,
		storages:             params.Storages,
		dumpedObjects:        make(map[string]string),
	}

	for _, src := range params.Sources {

		for _, key := range src.ExtraKeys {
			if matched, _ := regexp.MatchString(`(-f|--file)`, key); matched {
				return nil, fmt.Errorf("It is forbidden to use the \"--file|-f\" parameter as part of extra_keys. ")
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

				job.databasesList = append(job.databasesList, db)

				var ignoreTables []string
				compRegEx := regexp.MustCompile(`^(?P<db>` + db + `)\.(?P<table>.*$)`)
				for _, excl := range src.Excludes {
					if match := compRegEx.FindStringSubmatch(excl); len(match) > 0 {
						exclTable := "--exclude-table=" + match[2]
						ignoreTables = append(ignoreTables, exclTable)
					}
				}
				targets = append(targets, target{
					dbName:       db,
					ignoreTables: ignoreTables,
				})

			}
			job.sources = append(job.sources, source{
				targets:   targets,
				connect:   dbConn,
				extraKeys: src.ExtraKeys,
				gzip:      src.Gzip,
				connUrl:   connUrl,
			})
		}
	}

	return job, nil
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
	return j.storages.CleanupOldBackups(appCtx, j.databasesList)
}

func (j *job) DoBackup(appCtx *appctx.AppContext, tmpDir string) (errs []error) {

	for _, src := range j.sources {

		for _, target := range src.targets {

			tmpBackupFullPath := misc.GetFileFullPath(tmpDir, target.dbName, "sql", "", src.gzip)

			err := createTmpBackup(appCtx, tmpBackupFullPath, src, target)
			if err != nil {
				appCtx.Log().Errorf("Failed to create temp backups %s by job %s", tmpBackupFullPath, j.name)
				errs = append(errs, err...)
				continue
			} else {
				appCtx.Log().Infof("Created temp backups %s by job %s", tmpBackupFullPath, j.name)
			}

			j.dumpedObjects[target.dbName] = tmpBackupFullPath

			if j.deferredCopyingLevel <= 0 {
				errLst := j.storages.Delivery(appCtx, j.dumpedObjects)
				errs = append(errs, errLst...)
				j.dumpedObjects = make(map[string]string)
			}
		}
		if j.deferredCopyingLevel == 1 {
			errLst := j.storages.Delivery(appCtx, j.dumpedObjects)
			errs = append(errs, errLst...)
		}
	}

	if j.deferredCopyingLevel >= 2 {
		errLst := j.storages.Delivery(appCtx, j.dumpedObjects)
		errs = append(errs, errLst...)
	}

	return
}

func (j *job) Close() error {
	for _, src := range j.sources {
		_ = src.connect.Close()
	}
	return nil
}

func createTmpBackup(appCtx *appctx.AppContext, tmpBackupPath string, src source, target target) (errs []error) {

	backupWriter, err := misc.GetFileWriter(tmpBackupPath, src.gzip)
	if err != nil {
		appCtx.Log().Errorf("Unable to create tmp file. Error: %s", err)
		return append(errs, err)
	}
	defer backupWriter.Close()

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
