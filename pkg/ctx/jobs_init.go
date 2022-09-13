package ctx

import (
	"fmt"
	"sort"
	"strings"

	"github.com/hashicorp/go-multierror"

	"nxs-backup/interfaces"
	"nxs-backup/misc"
	"nxs-backup/modules/backup/desc_files"
	"nxs-backup/modules/backup/inc_files"
	"nxs-backup/modules/backup/mongodump"
	"nxs-backup/modules/backup/mysql"
	"nxs-backup/modules/backup/mysql_xtrabackup"
	"nxs-backup/modules/backup/psql"
	"nxs-backup/modules/backup/psql_basebackup"
	"nxs-backup/modules/backup/redis"
	"nxs-backup/modules/connectors/mongo_connect"
	"nxs-backup/modules/connectors/mysql_connect"
	"nxs-backup/modules/connectors/psql_connect"
	"nxs-backup/modules/connectors/redis_connect"
	"nxs-backup/modules/storage"
)

func jobsInit(cfgJobs []cfgJob, storages map[string]interfaces.Storage) ([]interfaces.Job, error) {
	var errs *multierror.Error
	var jobs []interfaces.Job

	for _, j := range cfgJobs {

		// jobs validation
		if len(j.JobName) == 0 {
			errs = multierror.Append(errs, fmt.Errorf("empty job name is unacceptable"))
			continue
		}
		if !misc.Contains(misc.AllowedJobTypes, j.JobType) {
			errs = multierror.Append(errs, fmt.Errorf("unknown job type \"%s\". Allowd types: %s", j.JobType, strings.Join(misc.AllowedJobTypes, ", ")))
			continue
		}

		jobStorages, needToMakeBackup, stErrs := initJobStorages(storages, j.StoragesOptions)
		if len(stErrs) > 0 {
			errs = multierror.Append(errs, stErrs...)
			continue
		}

		switch j.JobType {
		case "desc_files":
			var sources []desc_files.SourceParams
			for _, src := range j.Sources {
				sources = append(sources, desc_files.SourceParams{
					Name:        src.Name,
					Targets:     src.Targets,
					Excludes:    src.Excludes,
					Gzip:        src.Gzip,
					SaveAbsPath: src.SaveAbsPath,
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
				errs = multierror.Append(errs, err)
				continue
			}

			jobs = append(jobs, job)

		case "inc_files":
			var sources []inc_files.SourceParams
			for _, src := range j.Sources {
				sources = append(sources, inc_files.SourceParams{
					Name:        src.Name,
					Targets:     src.Targets,
					Excludes:    src.Excludes,
					Gzip:        src.Gzip,
					SaveAbsPath: src.SaveAbsPath,
				})
			}

			job, err := inc_files.Init(inc_files.JobParams{
				Name:                 j.JobName,
				TmpDir:               j.TmpDir,
				MetadataDir:          j.IncMetadataDir,
				SafetyBackup:         j.SafetyBackup,
				DeferredCopyingLevel: j.DeferredCopyingLevel,
				Storages:             jobStorages,
				Sources:              sources,
			})
			if err != nil {
				errs = multierror.Append(errs, err)
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
					Name:      src.Name,
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
				errs = multierror.Append(errs, err)
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
					Name:      src.Name,
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
				errs = multierror.Append(errs, err)
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
					Name:      src.Name,
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
				errs = multierror.Append(errs, err)
				continue
			}
			jobs = append(jobs, job)

		case "postgresql_basebackup":
			var sources []psql_basebackup.SourceParams

			for _, src := range j.Sources {
				var extraKeys []string
				if len(src.ExtraKeys) > 0 {
					extraKeys = strings.Split(src.ExtraKeys, " ")
				}

				sources = append(sources, psql_basebackup.SourceParams{
					ConnectParams: psql_connect.Params{
						User:    src.Connect.DBUser,
						Passwd:  src.Connect.DBPassword,
						Host:    src.Connect.DBHost,
						Port:    src.Connect.DBPort,
						Socket:  src.Connect.Socket,
						SSLMode: src.Connect.SSLMode,
					},
					Name:      src.Name,
					Gzip:      src.Gzip,
					IsSlave:   src.IsSlave,
					ExtraKeys: extraKeys,
				})
			}

			job, err := psql_basebackup.Init(psql_basebackup.JobParams{
				Name:                 j.JobName,
				TmpDir:               j.TmpDir,
				NeedToMakeBackup:     needToMakeBackup,
				SafetyBackup:         j.SafetyBackup,
				DeferredCopyingLevel: j.DeferredCopyingLevel,
				Storages:             jobStorages,
				Sources:              sources,
			})
			if err != nil {
				errs = multierror.Append(errs, err)
				continue
			}
			jobs = append(jobs, job)

		case "mongodb":
			var sources []mongodump.SourceParams

			for _, src := range j.Sources {
				var extraKeys []string
				if len(src.ExtraKeys) > 0 {
					extraKeys = strings.Split(src.ExtraKeys, " ")
				}

				sources = append(sources, mongodump.SourceParams{
					ConnectParams: mongo_connect.Params{
						User:              src.Connect.DBUser,
						Passwd:            src.Connect.DBPassword,
						Host:              src.Connect.DBHost,
						Port:              src.Connect.DBPort,
						RSName:            src.Connect.MongoRSName,
						RSAddr:            src.Connect.MongoRSAddr,
						ConnectionTimeout: src.Connect.ConnectTimeout,
					},
					Name:               src.Name,
					Gzip:               src.Gzip,
					ExtraKeys:          extraKeys,
					TargetDBs:          src.TargetDbs,
					ExcludeDBs:         src.ExcludeDbs,
					ExcludeCollections: src.ExcludeCollections,
				})
			}

			job, err := mongodump.Init(mongodump.JobParams{
				Name:                 j.JobName,
				TmpDir:               j.TmpDir,
				NeedToMakeBackup:     needToMakeBackup,
				SafetyBackup:         j.SafetyBackup,
				DeferredCopyingLevel: j.DeferredCopyingLevel,
				Storages:             jobStorages,
				Sources:              sources,
			})
			if err != nil {
				errs = multierror.Append(errs, err)
				continue
			}
			jobs = append(jobs, job)

		case "redis":
			var sources []redis.SourceParams

			for _, src := range j.Sources {
				sources = append(sources, redis.SourceParams{
					ConnectParams: redis_connect.Params{
						Passwd: src.Connect.DBPassword,
						Host:   src.Connect.DBHost,
						Port:   src.Connect.DBPort,
						Socket: src.Connect.Socket,
					},
					Name: src.Name,
					Gzip: src.Gzip,
				})
			}

			job, err := redis.Init(redis.JobParams{
				Name:                 j.JobName,
				TmpDir:               j.TmpDir,
				NeedToMakeBackup:     needToMakeBackup,
				SafetyBackup:         j.SafetyBackup,
				DeferredCopyingLevel: j.DeferredCopyingLevel,
				Storages:             jobStorages,
				Sources:              sources,
			})
			if err != nil {
				errs = multierror.Append(errs, err)
				continue
			}
			jobs = append(jobs, job)

		// "external" as default
		default:

		}
	}

	return jobs, errs.ErrorOrNil()
}

func initJobStorages(storages map[string]interfaces.Storage, opts []storageOpts) (jobStorages interfaces.Storages, needToMakeBackup bool, errs []error) {

	for _, stOpts := range opts {

		// storages validation
		s, ok := storages[stOpts.StorageName]
		if !ok {
			errs = append(errs, fmt.Errorf("unknown storage name: %s", stOpts.StorageName))
			continue
		}

		if stOpts.Retention.Days < 0 || stOpts.Retention.Weeks < 0 || stOpts.Retention.Months < 0 {
			errs = append(errs, fmt.Errorf("retention period can't be negative"))
		}

		st := s.Clone()
		st.SetBackupPath(stOpts.BackupPath)
		st.SetRetention(storage.Retention(stOpts.Retention))

		if storage.GetNeedToMakeBackup(stOpts.Retention.Days, stOpts.Retention.Weeks, stOpts.Retention.Months) {
			needToMakeBackup = true
		}

		jobStorages = append(jobStorages, st)
	}

	// sorting storages for installing local as last
	if len(jobStorages) > 1 {
		sort.Sort(jobStorages)
	}

	return
}