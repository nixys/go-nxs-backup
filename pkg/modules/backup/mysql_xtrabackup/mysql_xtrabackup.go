package mysql_xtrabackup

import (
	"archive/tar"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"

	"github.com/jmoiron/sqlx"
	appctx "github.com/nixys/nxs-go-appctx/v2"

	"nxs-backup/interfaces"
	"nxs-backup/misc"
	"nxs-backup/modules/backend/exec_cmd"
	"nxs-backup/modules/backend/mysql_connect"
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
	connect   *sqlx.DB
	authFile  string
	targets   []target
	extraKeys []string
	gzip      bool
	isSlave   bool
	prepare   bool
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
	Prepare       bool
}

func Init(jp JobParams) (*job, error) {

	// check if xtrabackup available
	_, err := exec_cmd.Exec("xtrabackup", "--version")
	if err != nil {
		return nil, fmt.Errorf("failed to check xtrabackup version. Please check that `xtrabackup` installed. Error: %s", err)
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

		dbConn, authFile, err := mysql_connect.GetConnectAndCnfFile(src.ConnectParams, "xtrabackup")
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

				j.backupsList = append(j.backupsList, src.Name+"/"+db)

				var ignoreTables []string
				compRegEx := regexp.MustCompile(`^(?P<db>` + db + `)\.(?P<table>.*$)`)
				for _, excl := range src.Excludes {
					if matched, _ := regexp.MatchString(`^\^`+db+`\[\.\].*$`, excl); matched {
						ignoreTables = append(ignoreTables, "--tables-exclude="+excl)
					} else if match := compRegEx.FindStringSubmatch(excl); len(match) > 0 {
						exclTable := "--tables-exclude=^" + db + "[.]" + match[2]
						ignoreTables = append(ignoreTables, exclTable)
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
				prepare:   src.Prepare,
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

			tmpBackupFile := misc.GetFileFullPath(tmpDir, src.name+"_"+tgt.dbName, "tar", "", src.gzip)

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

	var (
		stderr, stdout          bytes.Buffer
		backupArgs, prepareArgs []string
	)

	tmpXtrabackupPath := path.Join(path.Dir(tmpBackupFile), "xtrabackup_"+target.dbName+"_"+misc.GetDateTimeNow(""))

	backupWriter, err := misc.GetFileWriter(tmpBackupFile, src.gzip)
	if err != nil {
		appCtx.Log().Errorf("Unable to create tmp file. Error: %s", err)
		return append(errs, err)
	}
	defer func() { _ = backupWriter.Close() }()

	// define commands args with auth options
	backupArgs = append(backupArgs, "--defaults-file="+src.authFile)
	prepareArgs = backupArgs
	// add backup options
	backupArgs = append(backupArgs, "--backup", "--target-dir="+tmpXtrabackupPath)
	backupArgs = append(backupArgs, "--databases="+target.dbName)
	if len(target.ignoreTables) > 0 {
		backupArgs = append(backupArgs, target.ignoreTables...)
	}
	if src.isSlave {
		backupArgs = append(backupArgs, "--safe-slave-backup")
	}
	// add extra backup options
	if len(src.extraKeys) > 0 {
		backupArgs = append(backupArgs, src.extraKeys...)
	}

	cmd := exec.Command("xtrabackup", backupArgs...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err = cmd.Start(); err != nil {
		appCtx.Log().Errorf("Unable to start xtrabackup. Error: %s", err)
		errs = append(errs, err)
		return
	}
	appCtx.Log().Infof("Starting `%s` dump", target.dbName)

	if err = cmd.Wait(); err != nil {
		appCtx.Log().Errorf("Unable to dump `%s`. Error: %s", target.dbName, err)
		appCtx.Log().Error(stderr)
		errs = append(errs, err)
		return
	}

	if err = checkXtrabackupStatus(stderr.String()); err != nil {
		_ = os.WriteFile("/home/r.andreev/Projects/NxsProjects/nxs-backup/tmp/test/file.log", stderr.Bytes(), 0644)
		appCtx.Log().Errorf("Dump create fail. Error: %s", err)
		errs = append(errs, err)
		return
	}

	stdout.Reset()
	stderr.Reset()

	if src.prepare {
		// add prepare options
		prepareArgs = append(prepareArgs, "--prepare", "--export", "--target-dir="+tmpXtrabackupPath)
		cmd = exec.Command("xtrabackup", prepareArgs...)
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr

		if err = cmd.Run(); err != nil {
			appCtx.Log().Errorf("Unable to run xtrabackup. Error: %s", err)
			appCtx.Log().Error(stderr)
			errs = append(errs, err)
			return
		}

		if err = checkXtrabackupStatus(stderr.String()); err != nil {
			appCtx.Log().Errorf("Xtrabackup prepare fail. Error: %s", err)
			errs = append(errs, err)
			return
		}

		stdout.Reset()
		stderr.Reset()
	}

	tarWriter := tar.NewWriter(backupWriter)
	defer func() { _ = tarWriter.Close() }()

	err = targz.TarDirectory(tmpXtrabackupPath, tarWriter, filepath.Dir(tmpXtrabackupPath))
	if err != nil {
		appCtx.Log().Errorf("Unable to make tar: %s", err)
		errs = append(errs, err)
		return
	}

	appCtx.Log().Infof("Dump of `%s` completed", target.dbName)

	if err = os.RemoveAll(tmpXtrabackupPath); err != nil {
		appCtx.Log().Warnf("Failed to delete tmp xtrabackup dump directory: %s", err)
	}

	return
}

func checkXtrabackupStatus(out string) error {
	if matched, _ := regexp.MatchString(`.*completed OK!\n$`, out); matched {
		return nil
	}

	return fmt.Errorf("xtrabackup finished not success. Please check result:\n%s", out)
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
