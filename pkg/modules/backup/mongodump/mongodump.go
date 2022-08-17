package mongodump

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"regexp"

	appctx "github.com/nixys/nxs-go-appctx/v2"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"nxs-backup/interfaces"
	"nxs-backup/misc"
	"nxs-backup/modules/backend/exec_cmd"
	"nxs-backup/modules/backend/targz"
	"nxs-backup/modules/connectors/mongo_connect"
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
	connect   *mongo.Client
	dsn       string
	targets   []target
	extraKeys []string
	gzip      bool
}

type target struct {
	dbName            string
	ignoreCollections []string
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
	Name               string
	ConnectParams      mongo_connect.Params
	TargetDBs          []string
	ExcludeDBs         []string
	ExcludeCollections []string
	ExtraKeys          []string
	Gzip               bool
}

func Init(jp JobParams) (*job, error) {

	// check if mysqldump available
	_, err := exec_cmd.Exec("mongodump", "--version")
	if err != nil {
		return nil, fmt.Errorf("Job `%s` init failed. Failed to check mongodump version. Please check that `mongodump` installed. Error: %s ", jp.Name, err)
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

		conn, dsn, err := mongo_connect.GetConnectAndDSN(src.ConnectParams)
		if err != nil {
			return nil, fmt.Errorf("Job `%s` init failed. MongoDB connect error: %s ", jp.Name, err)
		}

		// fetch all databases
		var databases []string
		databases, err = conn.ListDatabaseNames(context.TODO(), bson.D{})
		if err != nil {
			return nil, fmt.Errorf("Job `%s` init failed. Unable to list databases. Error: %s ", jp.Name, err)
		}

		for _, db := range databases {
			if misc.Contains(src.ExcludeDBs, db) {
				continue
			}
			var targets []target
			if misc.Contains(src.TargetDBs, "all") || misc.Contains(src.TargetDBs, db) {

				j.dumpPathsList = append(j.dumpPathsList, src.Name+"/"+db)

				var ignoreCollections []string
				compRegEx := regexp.MustCompile(`^(?P<db>` + db + `)\.(?P<collection>.*$)`)
				for _, excl := range src.ExcludeCollections {
					if match := compRegEx.FindStringSubmatch(excl); len(match) > 0 {
						ignoreCollections = append(ignoreCollections, "--excludeCollection="+match[2])
					}
				}
				targets = append(targets, target{
					dbName:            db,
					ignoreCollections: ignoreCollections,
				})

			}
			j.sources = append(j.sources, source{
				name:      src.Name,
				targets:   targets,
				connect:   conn,
				dsn:       dsn,
				extraKeys: src.ExtraKeys,
				gzip:      src.Gzip,
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
	return "mongodb"
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

			tmpBackupFile := misc.GetFileFullPath(tmpDir, src.name+"_"+tgt.dbName, "tar", "", src.gzip)

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

func createTmpBackup(appCtx *appctx.AppContext, tmpBackupFile string, src source, target target) (errs []error) {

	tmpMongodumpPath := path.Join(path.Dir(tmpBackupFile), "mongodump_"+src.name+"_"+misc.GetDateTimeNow(""))

	var args []string
	// define command args
	// auth url
	args = append(args, "--uri="+src.dsn)
	args = append(args, "--authenticationDatabase=admin")
	// add db name
	args = append(args, "--db="+target.dbName)
	// add collections exclude
	if len(target.ignoreCollections) > 0 {
		args = append(args, target.ignoreCollections...)
	}
	// add extra dump cmd options
	if len(src.extraKeys) > 0 {
		args = append(args, src.extraKeys...)
	}
	// set output
	args = append(args, "--out="+tmpMongodumpPath)

	var stderr, stdout bytes.Buffer
	cmd := exec.Command("mongodump", args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		appCtx.Log().Errorf("Unable to start mongodump. Error: %s", err)
		errs = append(errs, err)
		return
	}
	appCtx.Log().Infof("Starting a `%s` dump", target.dbName)

	if err := cmd.Wait(); err != nil {
		appCtx.Log().Errorf("Unable to dump `%s`. Error: %s", target.dbName, stderr.String())
		errs = append(errs, err)
		return
	}

	if err := targz.Tar(tmpMongodumpPath, tmpBackupFile, src.gzip, false, nil); err != nil {
		appCtx.Log().Errorf("Unable to make tar: %s", err)
		errs = append(errs, err)
		return
	}
	_ = os.RemoveAll(tmpMongodumpPath)

	appCtx.Log().Infof("Dump of `%s` completed", target.dbName)

	return
}

func (j *job) Close() error {
	for _, src := range j.sources {
		_ = src.connect.Disconnect(context.TODO())
	}
	for _, st := range j.storages {
		_ = st.Close()
	}
	return nil
}
