package ctx

import (
	"fmt"
	"sort"
	"strings"

	"nxs-backup/interfaces"
	"nxs-backup/misc"
	"nxs-backup/modules/backend/mysql_connect"
	"nxs-backup/modules/backend/psql_connect"
	"nxs-backup/modules/backup/desc_files"
	"nxs-backup/modules/backup/mysql"
	"nxs-backup/modules/backup/mysql_xtrabackup"
	"nxs-backup/modules/backup/psql"
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
			if misc.GetNeedToMakeBackup(stOpts.Retention.Days, stOpts.Retention.Weeks, stOpts.Retention.Months) {
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
			for _, src := range j.Sources {
				sources = append(sources, desc_files.SourceParams{
					Targets:  src.Targets,
					Excludes: src.Excludes,
					Gzip:     src.Gzip,
				})
			}

			job, err := desc_files.Init(desc_files.JobParams{
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
				var extraKeys []string
				if len(src.ExtraKeys) > 0 {
					extraKeys = strings.Split(src.ExtraKeys, " ")
				}

				sources = append(sources, mysql.SourceParams{
					ConnectParams: mysql_connect.Params{
						AuthFile: src.Connect.AuthFile,
						User:     src.Connect.DBUser,
						Passwd:   src.Connect.DBPassword,
						Host:     src.Connect.DBHost,
						Port:     src.Connect.DBPort,
						Socket:   src.Connect.Socket,
					},
					TargetDBs: src.Targets,
					Excludes:  src.Excludes,
					Gzip:      src.Gzip,
					IsSlave:   src.IsSlave,
					ExtraKeys: extraKeys,
				})
			}

			job, err := mysql.Init(mysql.JobParams{
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

		case "mysql_xtrabackup":
			var sources []mysql_xtrabackup.SourceParams

			for _, src := range j.Sources {
				var extraKeys []string
				if len(src.ExtraKeys) > 0 {
					extraKeys = strings.Split(src.ExtraKeys, " ")
				}

				sources = append(sources, mysql_xtrabackup.SourceParams{
					ConnectParams: mysql_connect.Params{
						AuthFile: src.Connect.AuthFile,
						User:     src.Connect.DBUser,
						Passwd:   src.Connect.DBPassword,
						Host:     src.Connect.DBHost,
						Port:     src.Connect.DBPort,
						Socket:   src.Connect.Socket,
					},
					TargetDBs: src.Targets,
					Excludes:  src.Excludes,
					Gzip:      src.Gzip,
					IsSlave:   src.IsSlave,
					Prepare:   src.PrepareXtrabackup,
					ExtraKeys: extraKeys,
				})
			}

			job, err := mysql_xtrabackup.Init(mysql_xtrabackup.JobParams{
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

		case "postgresql":
			var sources []psql.SourceParams

			for _, src := range j.Sources {
				var extraKeys []string
				if len(src.ExtraKeys) > 0 {
					extraKeys = strings.Split(src.ExtraKeys, " ")
				}

				sources = append(sources, psql.SourceParams{
					ConnectParams: psql_connect.Params{
						User:    src.Connect.DBUser,
						Passwd:  src.Connect.DBPassword,
						Host:    src.Connect.DBHost,
						Port:    src.Connect.DBPort,
						Socket:  src.Connect.Socket,
						SSLMode: src.Connect.SSLMode,
					},
					TargetDBs: src.Targets,
					Excludes:  src.Excludes,
					Gzip:      src.Gzip,
					IsSlave:   src.IsSlave,
					ExtraKeys: extraKeys,
				})
			}

			job, err := psql.Init(psql.JobParams{
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
