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
	"nxs-backup/modules/backend/targz"
	"nxs-backup/modules/connectors/mysql_connect"
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
	connect      *sqlx.DB
	authFile     string
	dbName       string
	ignoreTables []string
	extraKeys    []string
	isSlave      bool
	gzip         bool
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
		targets:              make(map[string]target),
		dumpedObjects:        make(map[string]interfaces.DumpObject),
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
			if misc.Contains(src.TargetDBs, "all") || misc.Contains(src.TargetDBs, db) {

				var ignoreTables []string
				for _, excl := range src.Excludes {
					if matched, _ := regexp.MatchString(`^`+db+`\..*$`, excl); matched {
						ignoreTables = append(ignoreTables, "--ignore-table="+excl)
					}
				}
				j.targets[src.Name+"/"+db] = target{
					connect:      dbConn,
					authFile:     authFile,
					dbName:       db,
					ignoreTables: ignoreTables,
					extraKeys:    src.ExtraKeys,
					gzip:         src.Gzip,
					isSlave:      src.IsSlave,
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
	return "mysql"
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

		tmpBackupFile := misc.GetFileFullPath(tmpDir, ofsPart, "sql", "", tgt.gzip)

		errList := createTmpBackup(appCtx, tmpBackupFile, tgt)
		if errList != nil {
			appCtx.Log().Errorf("Job %s. Unable to create temp backups %s", j.name, tmpBackupFile)
			errs = append(errs, errList...)
			continue
		} else {
			appCtx.Log().Infof("Job %s. Created temp backups %s", j.name, tmpBackupFile)
		}

		j.dumpedObjects[ofsPart] = interfaces.DumpObject{TmpFile: tmpBackupFile}

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

func createTmpBackup(appCtx *appctx.AppContext, tmpBackupFile string, target target) (errs []error) {

	backupWriter, err := targz.GetFileWriter(tmpBackupFile, target.gzip)
	if err != nil {
		appCtx.Log().Errorf("Unable to create tmp file. Error: %s", err)
		return append(errs, err)
	}
	defer func() { _ = backupWriter.Close() }()

	if target.isSlave {
		_, err := target.connect.Exec("STOP SLAVE")
		if err != nil {
			appCtx.Log().Errorf("Unable to stop slave. Error: %s", err)
			errs = append(errs, err)
			return
		}
		appCtx.Log().Infof("Slave stopped")
		defer func() {
			_, err = target.connect.Exec("START SLAVE")
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
	args = append(args, "--defaults-extra-file="+target.authFile)
	// add tables exclude
	if len(target.ignoreTables) > 0 {
		args = append(args, target.ignoreTables...)
	}
	// add extra dump cmd options
	if len(target.extraKeys) > 0 {
		args = append(args, target.extraKeys...)
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

	appCtx.Log().Infof("Dump of `%s` completed", target.dbName)

	return
}

func (j *job) Close() error {
	for _, tgt := range j.targets {
		_ = os.Remove(tgt.authFile)
		_ = tgt.connect.Close()
	}
	for _, st := range j.storages {
		_ = st.Close()
	}
	return nil
}
