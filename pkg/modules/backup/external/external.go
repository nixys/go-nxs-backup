package external

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os/exec"

	appctx "github.com/nixys/nxs-go-appctx/v2"

	"nxs-backup/interfaces"
)

type job struct {
	name             string
	dumpCmd          string
	args             []string
	envs             map[string]string
	needToMakeBackup bool
	safetyBackup     bool
	storages         interfaces.Storages
	dumpedObjects    map[string]interfaces.DumpObject
}

type JobParams struct {
	Name             string
	DumpCmd          string
	Args             []string
	Envs             map[string]string
	NeedToMakeBackup bool
	SafetyBackup     bool
	Storages         interfaces.Storages
}

func Init(jp JobParams) (interfaces.Job, error) {

	return &job{
		name:             jp.Name,
		dumpCmd:          jp.DumpCmd,
		args:             jp.Args,
		envs:             jp.Envs,
		needToMakeBackup: jp.NeedToMakeBackup,
		safetyBackup:     jp.SafetyBackup,
		storages:         jp.Storages,
		dumpedObjects:    make(map[string]interfaces.DumpObject),
	}, nil
}

func (j *job) GetName() string {
	return j.name
}

func (j *job) GetTempDir() string {
	return ""
}

func (j *job) GetType() string {
	return "external"
}

func (j *job) GetTargetOfsList() []string {
	return []string{""}
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

func (j *job) DeleteOldBackups(appCtx *appctx.AppContext, ofsPath string) error {
	return j.storages.DeleteOldBackups(appCtx, j, ofsPath)
}

func (j *job) CleanupTmpData(appCtx *appctx.AppContext) error {
	return j.storages.CleanupTmpData(appCtx, j)
}

func (j *job) DoBackup(appCtx *appctx.AppContext, _ string) (err error) {

	var stderr, stdout bytes.Buffer

	defer func() {
		if err != nil {
			appCtx.Log().Errorf("Failed to create temp backup by job %s", j.name)
		}
	}()

	cmd := exec.Command(j.dumpCmd, j.args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if len(j.envs) > 0 {
		var envs []string
		for k, v := range j.envs {
			envs = append(envs, fmt.Sprintf("%s=%s", k, v))
		}
		cmd.Env = envs
	}

	if err = cmd.Start(); err != nil {
		appCtx.Log().Errorf("Unable to start %s. Error: %s", j.dumpCmd, err)
		return err
	}
	appCtx.Log().Infof("Starting of `%s`", j.dumpCmd)

	if err = cmd.Wait(); err != nil {
		appCtx.Log().Errorf("Unable to finish `%s`. Error: %s", j.dumpCmd, stderr.String())
		return err
	}

	var out struct {
		FullPath string `json:"full_path"`
	}
	err = json.Unmarshal(stdout.Bytes(), &out)
	if err != nil {
		appCtx.Log().Errorf("Unable to parse execution result. Error: %s", stderr.String())
		return err
	}

	appCtx.Log().Infof("Dumping completed")
	appCtx.Log().Infof("Created temp backup %s by job %s", out.FullPath, j.name)

	j.dumpedObjects[j.name] = interfaces.DumpObject{TmpFile: out.FullPath}

	return j.storages.Delivery(appCtx, j)
}

func (j *job) Close() error {
	for _, st := range j.storages {
		_ = st.Close()
	}
	return nil
}
