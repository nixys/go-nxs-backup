package mysql

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"regexp"

	"github.com/jmoiron/sqlx"
	appctx "github.com/nixys/nxs-go-appctx/v2"

	"nxs-backup/interfaces"
	"nxs-backup/misc"
	"nxs-backup/modules/backend/exec_cmd"
	"nxs-backup/modules/backend/mysql_connect"
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
	connect   *sqlx.DB
	authFile  string
	targets   []target
	extraKeys []string
	gzip      bool
	isSlave   bool
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
	ConnectParams mysql_connect.Params
	TargetDBs     []string
	Excludes      []string
	ExtraKeys     []string
	Gzip          bool
	IsSlave       bool
}

func Init(jp JobParams) (*job, error) {

	// check if mysqldump available
	_, err := exec_cmd.Exec("mysqldump", "--version")
	if err != nil {
		return nil, fmt.Errorf("Job `%s` init failed. Failed to check mysqldump version. Please check that `mysqldump` installed. Error: %s ", jp.Name, err)
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

		dbConn, authFile, err := mysql_connect.GetConnectAndCnfFile(src.ConnectParams, "mysqldump")
		if err != nil {
			return nil, fmt.Errorf("Job `%s` init failed. MySQL connect error: %s ", jp.Name, err)
		}

		// fetch all databases
		var databases []string
		err = dbConn.Select(&databases, "show databases")
		if err != nil {
			return nil, fmt.Errorf("Job `%s` init failed. Unable to list databases. Error: %s ", jp.Name, err)
		}

		for _, db := range databases {
			if misc.Contains(src.Excludes, db) {
				continue
			}
			var targets []target
			if misc.Contains(src.TargetDBs, "all") || misc.Contains(src.TargetDBs, db) {

				j.backupsList = append(j.backupsList, src.Name+"/"+db)

				var ignoreTables []string
				pattern := `^` + db + `\..*$`
				for _, excl := range src.Excludes {
					if matched, _ := regexp.MatchString(pattern, excl); matched {
						ignoreTables = append(ignoreTables, "--ignore-table="+excl)
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
				authFile:  authFile,
				extraKeys: src.ExtraKeys,
				gzip:      src.Gzip,
				isSlave:   src.IsSlave,
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

		for _, tgt := range src.targets {

			tmpBackupFile := misc.GetFileFullPath(tmpDir, src.name+"_"+tgt.dbName, "sql", "", src.gzip)

			err := createTmpBackup(appCtx, tmpBackupFile, src, tgt)
			if err != nil {
				appCtx.Log().Errorf("Job %s. Unable to create temp backups %s", j.name, tmpBackupFile)
				errs = append(errs, err...)
				continue
			} else {
				appCtx.Log().Infof("Job %s. Created temp backups %s", j.name, tmpBackupFile)
			}

			j.dumpedObjects[src.name+"/"+tgt.dbName] = tmpBackupFile

			if j.deferredCopyingLevel <= 0 {
				errLst := j.storages.Delivery(appCtx, j.dumpedObjects)
				errs = append(errs, errLst...)
				j.dumpedObjects = make(map[string]string)
			}
		}
		if j.deferredCopyingLevel == 1 {
			errLst := j.storages.Delivery(appCtx, j.dumpedObjects)
			errs = append(errs, errLst...)
			j.dumpedObjects = make(map[string]string)
		}
	}

	if j.deferredCopyingLevel >= 2 {
		errLst := j.storages.Delivery(appCtx, j.dumpedObjects)
		errs = append(errs, errLst...)
	}

	return
}

func createTmpBackup(appCtx *appctx.AppContext, tmpBackupFile string, src source, target target) (errs []error) {

	backupWriter, err := misc.GetFileWriter(tmpBackupFile, src.gzip)
	if err != nil {
		appCtx.Log().Errorf("Unable to create tmp file. Error: %s", err)
		return append(errs, err)
	}
	defer backupWriter.Close()

	if src.isSlave {
		_, err := src.connect.Exec("STOP SLAVE")
		if err != nil {
			appCtx.Log().Errorf("Unable to stop slave. Error: %s", err)
			errs = append(errs, err)
			return
		}
		appCtx.Log().Infof("Slave stopped")
		defer func() {
			_, err = src.connect.Exec("START SLAVE")
			if err != nil {
				appCtx.Log().Errorf("Unable to start slave. Error: %s", err)
				errs = append(errs, err)
			} else {
				appCtx.Log().Infof("Slave started")
			}
		}()
	}

	var args []string
	// define command args with auth options
	args = append(args, "--defaults-extra-file="+src.authFile)
	// add tables exclude
	if len(target.ignoreTables) > 0 {
		args = append(args, target.ignoreTables...)
	}
	// add extra dump cmd options
	if len(src.extraKeys) > 0 {
		args = append(args, src.extraKeys...)
	}
	// add db name
	args = append(args, target.dbName)

	var stderr bytes.Buffer
	cmd := exec.Command("mysqldump", args...)
	cmd.Stdout = backupWriter
	cmd.Stderr = &stderr

	if err = cmd.Start(); err != nil {
		appCtx.Log().Errorf("Unable to start mysqldump. Error: %s", err)
		errs = append(errs, err)
		return
	}
	appCtx.Log().Infof("Starting a `%s` dump", target.dbName)

	if err = cmd.Wait(); err != nil {
		appCtx.Log().Errorf("Unable to dump `%s`. Error: %s", target.dbName, stderr.String())
		errs = append(errs, err)
		return
	}

	stderr.Reset()

	appCtx.Log().Infof("Dump of `%s` completed", target.dbName)

	return
}

func (j *job) Close() error {
	for _, src := range j.sources {
		_ = os.Remove(src.authFile)
		_ = src.connect.Close()
	}
	for _, st := range j.storages {
		_ = st.Close()
	}
	return nil
}
