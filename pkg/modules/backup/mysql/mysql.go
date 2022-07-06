package mysql

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"regexp"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	appctx "github.com/nixys/nxs-go-appctx/v2"
	"gopkg.in/ini.v1"

	"nxs-backup/interfaces"
	"nxs-backup/misc"
	"nxs-backup/modules/backend/exec_cmd"
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
	targets   []target
	isSlave   bool
	gzip      bool
	extraKeys []string
	connect   *sqlx.DB
	authFile  string
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
	ConnectParams
	TargetDBs []string
	Excludes  []string
	ExtraKeys []string
	Gzip      bool
	IsSlave   bool
}

type ConnectParams struct {
	AuthFile string // Path to auth file
	User     string // Username
	Passwd   string // Password (requires User)
	Host     string // Network host
	Port     string // Network port
	Socket   string // Socket path
}

func Init(params JobParams) (*mysqlJob, error) {

	// check if mysqldump available
	_, err := exec_cmd.Exec("mysqldumpl", "--version")
	if err != nil {
		return nil, fmt.Errorf("failed to check mysqldump version. Please check that `mysqldump` installed. Error: %s", err)
	}

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

		dbConn, authFile, err := getMysqlConnect(src.ConnectParams)
		if err != nil {
			return nil, err
		}

		// fetch all databases
		var databases []string
		err = dbConn.Select(&databases, "show databases")
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
			job.sources = append(job.sources, source{
				targets:   targets,
				connect:   dbConn,
				authFile:  authFile,
				extraKeys: src.ExtraKeys,
				gzip:      src.Gzip,
				isSlave:   src.IsSlave,
			})
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

		for _, target := range src.targets {

			tmpBackupFullPath := misc.GetBackupFullPath(tmpDir, target.dbName, "sql", "", src.gzip)

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

func (j *mysqlJob) Close() error {
	for _, src := range j.sources {
		_ = os.Remove(src.authFile)
		_ = src.connect.Close()
	}
	return nil
}

func createTmpBackup(appCtx *appctx.AppContext, tmpBackupPath string, src source, target target) (errs []error) {

	backupWriter, err := misc.GetBackupWriter(tmpBackupPath, src.gzip)
	defer backupWriter.Close()
	if err != nil {
		appCtx.Log().Errorf("Unable to create tmp file. Error: %s", err)
		return append(errs, err)
	}

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
		appCtx.Log().Errorf("Unable to dump `%s`. Error: %s", target.dbName, err)
		errs = append(errs, err)
		return
	}

	appCtx.Log().Infof("Dump of `%s` completed", target.dbName)

	return
}

func getMysqlConnect(conn ConnectParams) (*sqlx.DB, string, error) {

	dumpAuthCfg := ini.Empty()
	_ = dumpAuthCfg.NewSections("mysqldump")

	if conn.AuthFile != "" {
		authCfg, err := ini.LoadSources(ini.LoadOptions{AllowBooleanKeys: true}, conn.AuthFile)
		if err != nil {
			return nil, "", err
		}

		for _, sName := range []string{"mysql", "client", "mysqldump", ""} {
			s, err := authCfg.GetSection(sName)
			if err != nil {
				continue
			}
			if user := s.Key("user").MustString(""); user != "" {
				conn.User = user
				_, _ = dumpAuthCfg.Section("mysqldump").NewKey("user", user)
			}
			if pass := s.Key("password").MustString(""); pass != "" {
				conn.Passwd = pass
				_, _ = dumpAuthCfg.Section("mysqldump").NewKey("password", pass)
			}
			if socket := s.Key("socket").MustString(""); socket != "" {
				conn.Socket = socket
				_, _ = dumpAuthCfg.Section("mysqldump").NewKey("socket", socket)
			}
			if host := s.Key("host").MustString(""); host != "" {
				conn.Host = host
				_, _ = dumpAuthCfg.Section("mysqldump").NewKey("host", host)
			}
			if port := s.Key("port").MustString(""); port != "" {
				conn.Port = port
				_, _ = dumpAuthCfg.Section("mysqldump").NewKey("port", port)
			}
			break
		}
	} else {
		if conn.User != "" {
			_, _ = dumpAuthCfg.Section("mysqldump").NewKey("user", conn.User)
		}
		if conn.Passwd != "" {
			_, _ = dumpAuthCfg.Section("mysqldump").NewKey("password", conn.Passwd)
		}
		if conn.Socket != "" {
			_, _ = dumpAuthCfg.Section("mysqldump").NewKey("socket", conn.Socket)
		}
		if conn.Host != "" {
			_, _ = dumpAuthCfg.Section("mysqldump").NewKey("host", conn.Host)
		}
		if conn.Port != "" {
			_, _ = dumpAuthCfg.Section("mysqldump").NewKey("port", conn.Port)
		}
	}

	authFile := misc.GetBackupFullPath("/tmp", "my_cnf", "ini", misc.RandString(5), false)
	err := dumpAuthCfg.SaveTo(authFile)
	if err != nil {
		return nil, authFile, err
	}

	cfg := mysql.NewConfig()
	cfg.User = conn.User
	cfg.Passwd = conn.Passwd
	if conn.Socket != "" {
		cfg.Net = "unix"
		cfg.Addr = conn.Socket
	} else {
		cfg.Net = "tcp"
		cfg.Addr = fmt.Sprintf("%s:%s", conn.Host, conn.Port)
	}
	db, err := sqlx.Connect("mysql", cfg.FormatDSN())

	return db, authFile, err
}
