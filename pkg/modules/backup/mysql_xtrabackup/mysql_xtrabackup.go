package mysql_xtrabackup

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path"
	"regexp"

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
	authFile     string
	dbName       string
	ignoreTables []string
	extraKeys    []string
	gzip         bool
	isSlave      bool
	prepare      bool
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
		targets:              make(map[string]target),
		dumpedObjects:        make(map[string]interfaces.DumpObject),
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
		_ = dbConn.Close()

		for _, db := range databases {
			if misc.Contains(src.Excludes, db) {
				continue
			}
			if misc.Contains(src.TargetDBs, "all") || misc.Contains(src.TargetDBs, db) {

				var ignoreTables []string
				compRegEx := regexp.MustCompile(`^(?P<db>` + db + `)\.(?P<table>.*$)`)
				for _, excl := range src.Excludes {
					if matched, _ := regexp.MatchString(`^\^`+db+`\[\.\].*$`, excl); matched {
						ignoreTables = append(ignoreTables, "--tables-exclude="+excl)
					} else if match := compRegEx.FindStringSubmatch(excl); len(match) > 0 {
						ignoreTables = append(ignoreTables, "--tables-exclude=^"+db+"[.]"+match[2])
					}
				}
				j.targets[src.Name+"/"+db] = target{
					authFile:     authFile,
					dbName:       db,
					ignoreTables: ignoreTables,
					extraKeys:    src.ExtraKeys,
					gzip:         src.Gzip,
					isSlave:      src.IsSlave,
					prepare:      src.Prepare,
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
	return "mysql_xtrabackup"
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

func (j *job) DeleteOldBackups(appCtx *appctx.AppContext, ofsPath string) []error {
	return j.storages.DeleteOldBackups(appCtx, j, ofsPath)
}

func (j *job) CleanupTmpData(appCtx *appctx.AppContext) error {
	return j.storages.CleanupTmpData(appCtx, j)
}

func (j *job) DoBackup(appCtx *appctx.AppContext, tmpDir string) (errs []error) {

	for ofsPart, tgt := range j.targets {

		tmpBackupFile := misc.GetFileFullPath(tmpDir, ofsPart, "tar", "", tgt.gzip)

		errList := createTmpBackup(appCtx, tmpBackupFile, tgt)
		if errList != nil {
			appCtx.Log().Errorf("Failed to create temp backups %s by job %s", tmpBackupFile, j.name)
			errs = append(errs, errList...)
			continue
		} else {
			appCtx.Log().Infof("Created temp backups %s by job %s", tmpBackupFile, j.name)
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

	var (
		stderr, stdout          bytes.Buffer
		backupArgs, prepareArgs []string
	)

	tmpXtrabackupPath := path.Join(path.Dir(tmpBackupFile), "xtrabackup_"+target.dbName+"_"+misc.GetDateTimeNow(""))

	// define commands args with auth options
	backupArgs = append(backupArgs, "--defaults-file="+target.authFile)
	prepareArgs = backupArgs
	// add backup options
	backupArgs = append(backupArgs, "--backup", "--target-dir="+tmpXtrabackupPath)
	backupArgs = append(backupArgs, "--databases="+target.dbName)
	if len(target.ignoreTables) > 0 {
		backupArgs = append(backupArgs, target.ignoreTables...)
	}
	if target.isSlave {
		backupArgs = append(backupArgs, "--safe-slave-backup")
	}
	// add extra backup options
	if len(target.extraKeys) > 0 {
		backupArgs = append(backupArgs, target.extraKeys...)
	}

	cmd := exec.Command("xtrabackup", backupArgs...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		appCtx.Log().Errorf("Unable to start xtrabackup. Error: %s", err)
		errs = append(errs, err)
		return
	}
	appCtx.Log().Infof("Starting `%s` dump", target.dbName)

	if err := cmd.Wait(); err != nil {
		appCtx.Log().Errorf("Unable to dump `%s`. Error: %s", target.dbName, err)
		appCtx.Log().Error(stderr)
		errs = append(errs, err)
		return
	}

	if err := checkXtrabackupStatus(stderr.String()); err != nil {
		_ = os.WriteFile("/home/r.andreev/Projects/NxsProjects/nxs-backup/tmp/test/file.log", stderr.Bytes(), 0644)
		appCtx.Log().Errorf("Dump create fail. Error: %s", err)
		errs = append(errs, err)
		return
	}

	stdout.Reset()
	stderr.Reset()

	if target.prepare {
		// add prepare options
		prepareArgs = append(prepareArgs, "--prepare", "--export", "--target-dir="+tmpXtrabackupPath)
		cmd = exec.Command("xtrabackup", prepareArgs...)
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr

		if err := cmd.Run(); err != nil {
			appCtx.Log().Errorf("Unable to run xtrabackup. Error: %s", err)
			appCtx.Log().Error(stderr)
			errs = append(errs, err)
			return
		}

		if err := checkXtrabackupStatus(stderr.String()); err != nil {
			appCtx.Log().Errorf("Xtrabackup prepare fail. Error: %s", err)
			errs = append(errs, err)
			return
		}
	}

	if err := targz.Tar(tmpXtrabackupPath, tmpBackupFile, target.gzip, false, nil); err != nil {
		appCtx.Log().Errorf("Unable to make tar: %s", err)
		errs = append(errs, err)
		return
	}
	_ = os.RemoveAll(tmpXtrabackupPath)

	appCtx.Log().Infof("Dump of `%s` completed", target.dbName)

	return
}

func checkXtrabackupStatus(out string) error {
	if matched, _ := regexp.MatchString(`.*completed OK!\n$`, out); matched {
		return nil
	}

	return fmt.Errorf("xtrabackup finished not success. Please check result:\n%s", out)
}

func (j *job) Close() error {
	for _, st := range j.storages {
		_ = st.Close()
	}
	return nil
}
