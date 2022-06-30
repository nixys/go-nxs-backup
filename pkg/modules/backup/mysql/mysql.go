package mysql

import (
	"database/sql"
	"github.com/aliakseiz/go-mysqldump"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	appctx "github.com/nixys/nxs-go-appctx/v2"

	"nxs-backup/interfaces"
	"nxs-backup/misc"
)

type Job struct {
	Name                 string
	TmpDir               string
	NeedToMakeBackup     bool
	SafetyBackup         bool
	DeferredCopyingLevel int
	Storages             interfaces.Storages
	Sources              []Source
	DumpedObjects        map[string]string
}

type Source struct {
	Target *mysqldump.Data
	Gzip   bool
}

type Params struct {
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
	Gzip        bool
	TargetDBs   map[string]*TargetDBParams
	ExcludedDBs []string
}

type TargetDBParams struct {
	IgnoreTables []string
	LockTables   bool
}

type ConnectParams struct {
	User   string // Username
	Passwd string // Password (requires User)
	Net    string // Network type
	Addr   string // Network address (requires Net)
	DBName string // Database name
}

func Init(params Params) (*Job, error) {

	job := &Job{
		Name:                 params.Name,
		TmpDir:               params.TmpDir,
		NeedToMakeBackup:     params.NeedToMakeBackup,
		SafetyBackup:         params.SafetyBackup,
		DeferredCopyingLevel: params.DeferredCopyingLevel,
		Storages:             params.Storages,
	}

	for _, source := range params.Sources {

		cfg := mysql.NewConfig()
		cfg.User = source.User
		cfg.Passwd = source.Passwd
		cfg.Net = source.Net
		cfg.Addr = source.Addr

		baseConn, err := sqlx.Connect("mysql", cfg.FormatDSN())
		defer baseConn.Close()
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
		for tdbName := range source.TargetDBs {
			targetDBsNames = append(targetDBsNames, tdbName)
		}

		for _, db := range databases {
			if misc.Contains(source.ExcludedDBs, db) {
				continue
			}
			if misc.Contains(targetDBsNames, "all") || misc.Contains(targetDBsNames, db) {
				scfg := cfg.Clone()
				scfg.DBName = db
				conn, err := sql.Open("mysql", scfg.FormatDSN())
				if err != nil {
					return nil, err
				}

				job.Sources = append(job.Sources, Source{
					Gzip: source.Gzip,
					Target: &mysqldump.Data{
						Connection:   conn,
						DBName:       db,
						IgnoreTables: source.TargetDBs[db].IgnoreTables,
						LockTables:   source.TargetDBs[db].LockTables,
					},
				})
			}
		}
	}

	return job, nil
}

func (j *Job) GetJobName() string {
	return j.Name
}

func (j *Job) GetJobType() string {
	return "databases"
}

func (j *Job) DoBackup(appCtx *appctx.AppContext) (errs []error) {
	return
}
