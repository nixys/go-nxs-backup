package ctx

import (
	"fmt"
	"gopkg.in/ini.v1"
	"sort"
	"strings"

	"nxs-backup/interfaces"
	"nxs-backup/misc"
	"nxs-backup/modules/backup/desc_files"
	"nxs-backup/modules/backup/mysql"
	"nxs-backup/modules/storage"
)

func jobsInit(cfgJobs []cfgJob, storages map[string]interfaces.Storage) (jobs []interfaces.Job, errs []error) {

	for _, j := range cfgJobs {

		// jobs validation
		if len(j.JobName) == 0 {
			errs = append(errs, fmt.Errorf("empty job name is unacceptable"))
			continue
		}
		if !misc.Contains(misc.AllowedJobTypes, j.JobType) {
			errs = append(errs, fmt.Errorf("unknown job type \"%s\". Allowd types: %s", j.JobType, strings.Join(misc.AllowedJobTypes, ", ")))
			continue
		}

		needToMakeBackup := false

		var jobStorages interfaces.Storages

		for _, stOpts := range j.StoragesOptions {

			// storages validation
			s, ok := storages[stOpts.StorageName]
			if !ok {
				errs = append(errs, fmt.Errorf("unknown storage name: %s", stOpts.StorageName))
				continue
			}

			if stOpts.Retention.Days < 0 || stOpts.Retention.Weeks < 0 || stOpts.Retention.Months < 0 {
				errs = append(errs, fmt.Errorf("retention period can't be negative"))
			}
			if misc.NeedToMakeBackup(stOpts.Retention.Days, stOpts.Retention.Weeks, stOpts.Retention.Months) {
				needToMakeBackup = true
			}

			s.SetBackupPath(stOpts.BackupPath)
			s.SetRetention(storage.Retention(stOpts.Retention))

			jobStorages = append(jobStorages, s)
		}

		// sorting storages for installing local as last
		if len(jobStorages) > 1 {
			sort.Sort(jobStorages)
		}

		switch j.JobType {
		case "desc_files":
			var sources []desc_files.SourceParams
			for _, s := range j.Sources {
				sources = append(sources, desc_files.SourceParams{
					Targets:  s.Targets,
					Excludes: s.Excludes,
					Gzip:     s.Gzip,
				})
			}

			job, err := desc_files.Init(desc_files.Params{
				Name:                 j.JobName,
				TmpDir:               j.TmpDir,
				NeedToMakeBackup:     needToMakeBackup,
				SafetyBackup:         j.SafetyBackup,
				DeferredCopyingLevel: j.DeferredCopyingLevel,
				Storages:             jobStorages,
				Sources:              sources,
			})
			if err != nil {
				errs = append(errs, err)
				continue
			}

			jobs = append(jobs, job)

		case "mysql":
			var sources []mysql.SourceParams

			for _, src := range j.Sources {
				connect, err := getMysqlConnectParams(src.Connect)
				if err != nil {
					errs = append(errs, err)
					continue
				}

				sources = append(sources, mysql.SourceParams{
					Gzip:          src.Gzip,
					ExcludedDBs:   src.ExcludeDbs,
					TargetDBs:     getMysqlTargetsParams(src),
					ConnectParams: connect,
				})
			}

			job, err := mysql.Init(mysql.Params{
				Name:                 j.JobName,
				TmpDir:               j.TmpDir,
				NeedToMakeBackup:     needToMakeBackup,
				SafetyBackup:         j.SafetyBackup,
				DeferredCopyingLevel: j.DeferredCopyingLevel,
				Storages:             jobStorages,
				Sources:              sources,
			})
			if err != nil {
				errs = append(errs, err)
				continue
			}
			jobs = append(jobs, job)

		// "external" as default
		default:

		}
	}

	return
}

func getMysqlTargetsParams(source cfgSource) (targets map[string]*mysql.TargetDBParams) {
	targets = make(map[string]*mysql.TargetDBParams)

	for _, db := range source.Targets {
		targets[db] = &mysql.TargetDBParams{
			LockTables: false,
		}
	}
	for _, excl := range source.Excludes {
		ex := strings.Split(excl, ".")
		if len(ex) == 2 {
			targets[ex[0]].IgnoreTables = append(targets[ex[0]].IgnoreTables, ex[1])
		}
	}
	return
}

func getMysqlConnectParams(conn cfgConnect) (mysql.ConnectParams, error) {

	if conn.AuthFile != "" {
		authCfg, err := ini.LoadSources(ini.LoadOptions{AllowBooleanKeys: true}, conn.AuthFile)
		if err != nil {
			return mysql.ConnectParams{}, err
		}

		for _, sName := range []string{"mysql", "client", "mysqldump", ""} {
			s, err := authCfg.GetSection(sName)
			if err != nil {
				continue
			}
			if user := s.Key("user").MustString(""); user != "" {
				conn.DBUser = user
			}
			if pass := s.Key("password").MustString(""); pass != "" {
				conn.DBPassword = pass
			}
			if socket := s.Key("socket").MustString(""); socket != "" {
				conn.Socket = socket
			}
			if host := s.Key("host").MustString(""); host != "" {
				conn.DBHost = host
			}
			if port := s.Key("port").MustString(""); port != "" {
				conn.DBPort = port
			}
			break
		}
	}

	out := mysql.ConnectParams{
		User:   conn.DBUser,
		Passwd: conn.DBPassword,
	}

	if conn.Socket != "" {
		out.Net = "unix"
		out.Addr = conn.Socket
	} else {
		out.Net = "tcp"
		out.Addr = fmt.Sprintf("%s:%s", conn.DBHost, conn.DBPort)
	}

	return out, nil
}
