package mysql

import (
	"github.com/aliakseiz/go-mysqldump"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	appctx "github.com/nixys/nxs-go-appctx/v2"
	"strings"

	"nxs-backup/interfaces"
	"nxs-backup/misc"
)

type mysqlJob struct {
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
	dbName       string
	connect      *sqlx.DB
	ignoreTables []string
	lockTables   bool
	gzip         bool
	isSlave      bool
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
	ConnectParams
	TargetDBs  []string
	Excludes   []string
	LockTables bool
	Gzip       bool
	IsSlave    bool
}

type ConnectParams struct {
	User   string // Username
	Passwd string // Password (requires User)
	Net    string // Network type
	Addr   string // Network address (requires Net)
	DBName string // Database name
}

func Init(params JobParams) (*mysqlJob, error) {

	job := &mysqlJob{
		name:                 params.Name,
		tmpDir:               params.TmpDir,
		needToMakeBackup:     params.NeedToMakeBackup,
		safetyBackup:         params.SafetyBackup,
		deferredCopyingLevel: params.DeferredCopyingLevel,
		storages:             params.Storages,
		dumpedObjects:        make(map[string]string),
	}

	for _, src := range params.Sources {

		cfg := mysql.NewConfig()
		cfg.User = src.User
		cfg.Passwd = src.Passwd
		cfg.Net = src.Net
		cfg.Addr = src.Addr

		baseConn, err := sqlx.Connect("mysql", cfg.FormatDSN())
		defer func() {
			_ = baseConn.Close()
		}()
		if err != nil {
			return nil, err
		}

		// fetch all databases
		var databases []string
		err = baseConn.Select(&databases, "show databases")
		if err != nil {
			return nil, err
		}

		var targetDBsNames []string
		for _, tdbName := range src.TargetDBs {
			targetDBsNames = append(targetDBsNames, tdbName)
		}

		for _, db := range databases {
			if misc.Contains(src.Excludes, db) {
				continue
			}
			if misc.Contains(targetDBsNames, "all") || misc.Contains(targetDBsNames, db) {
				scfg := cfg.Clone()
				scfg.DBName = db
				conn, err := sqlx.Connect("mysql", scfg.FormatDSN())
				if err != nil {
					return nil, err
				}

				job.databasesList = append(job.databasesList, db)

				var ignoreTables []string
				for _, excl := range src.Excludes {
					ex := strings.Split(excl, ".")
					if len(ex) == 2 {
						ignoreTables = append(ignoreTables, ex[1])
					}
				}

				job.sources = append(job.sources, source{
					connect:      conn,
					dbName:       db,
					ignoreTables: ignoreTables,
					lockTables:   src.LockTables,
					gzip:         src.Gzip,
					isSlave:      src.IsSlave,
				})
			}
		}
	}

	return job, nil
}

func (j *mysqlJob) GetName() string {
	return j.name
}

func (j *mysqlJob) GetTempDir() string {
	return j.tmpDir
}

func (j *mysqlJob) GetType() string {
	return "databases"
}

func (j *mysqlJob) IsBackupSafety() bool {
	return j.safetyBackup
}

func (j *mysqlJob) IsNeedToMakeBackup() bool {
	return j.needToMakeBackup
}

func (j *mysqlJob) CleanupOldBackups(appCtx *appctx.AppContext) []error {
	return j.storages.CleanupOldBackups(appCtx, j.databasesList)
}

func (j *mysqlJob) DoBackup(appCtx *appctx.AppContext, tmpDir string) (errs []error) {

	for _, src := range j.sources {

		tmpBackupFullPath := misc.GetBackupFullPath(tmpDir, src.dbName, "sql", "", src.gzip)

		err := createTmpBackup(appCtx, tmpBackupFullPath, src)
		if err != nil {
			appCtx.Log().Errorf("Failed to create temp backups %s by job %s", tmpBackupFullPath, j.name)
			errs = append(errs, err...)
			continue
		} else {
			appCtx.Log().Infof("Created temp backups %s by job %s", tmpBackupFullPath, j.name)
		}

		j.dumpedObjects[src.dbName] = tmpBackupFullPath

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

func createTmpBackup(appCtx *appctx.AppContext, tmpBackupPath string, src source) (errs []error) {
	backupWriter, err := misc.GetBackupWriter(tmpBackupPath, src.gzip)
	if err != nil {
		appCtx.Log().Errorf("Unable to create tmp file: %s", err)
		return append(errs, err)
	}

	target := &mysqldump.Data{
		Connection:   src.connect.DB,
		DBName:       src.dbName,
		IgnoreTables: src.ignoreTables,
		LockTables:   src.lockTables,
		Out:          backupWriter,
	}
	defer func() {
		_ = target.Close()
	}()

	if src.isSlave {
		_, err = src.connect.Exec("STOP SLAVE")
		if err != nil {
			appCtx.Log().Errorf("Unable to stop slave: %s", err)
			errs = append(errs, err)
			return
		}
		appCtx.Log().Infof("Slave stopped: %s", err)
		defer func() {
			_, err = src.connect.Exec("START SLAVE")
			if err != nil {
				appCtx.Log().Errorf("Unable to start slave: %s", err)
				errs = append(errs, err)
			}
		}()
	}

	err = target.Dump()
	if err != nil {
		appCtx.Log().Errorf("Unable to dump db: %s", err)
		errs = append(errs, err)
	}

	return
}
