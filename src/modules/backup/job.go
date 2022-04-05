package backup

import (
	"nxs-backup/interfaces"
	"nxs-backup/misc"
	"nxs-backup/modules/storage"
	"sort"
)

type JobSettings struct {
	JobName              string
	JobType              string
	TmpDir               string
	DumpCmd              string
	SafetyBackup         bool
	DeferredCopyingLevel int
	IncMonthsToStore     int
	Sources              []SourceSettings
	Storages             []StorageSettings
}

type StorageSettings struct {
	Enable     bool
	Storage    string
	BackupPath string
	Retention  RetentionSettings
}

type RetentionSettings struct {
	Days   int
	Weeks  int
	Months int
}

type SourceSettings struct {
	SpecialKeys        string
	Target             []string
	TargetDbs          []string
	TargetCollections  []string
	Excludes           []string
	ExcludeDbs         []string
	ExcludeCollections []string
	Gzip               bool
	SkipBackupRotate   bool
	ConnectSettings
}

type ConnectSettings struct {
	AuthFile   string
	DBHost     string
	DBPort     string
	Socket     string
	DBUser     string
	DBPassword string
	PathToConf string
}

func JobsInit(js []JobSettings) (jobs []interfaces.Job) {

	for _, j := range js {

		var sts []interfaces.Storage
		for _, s := range j.Storages {
			if s.Enable && misc.NeedToMakeBackup(s.Retention.Days, s.Retention.Weeks, s.Retention.Months) {
				switch s.Storage {
				// default = "local"
				default:
					sts = append(sts, storage.Local{
						BackupPath: s.BackupPath,
						Retention:  storage.Retention(s.Retention),
					})
				}
			}
		}
		sort.Sort(interfaces.SortByLocal(sts))

		switch j.JobType {
		case "desc_files":
			var srcs []DescFilesSource
			for _, s := range j.Sources {
				srcs = append(srcs, DescFilesSource{
					Targets:  s.Target,
					Excludes: s.Excludes,
					Gzip:     s.Gzip,
				})
			}

			jobs = append(jobs, DescFilesJob{
				JobName:              j.JobName,
				TmpDir:               j.TmpDir,
				DumpCmd:              j.DumpCmd,
				SafetyBackup:         j.SafetyBackup,
				DeferredCopyingLevel: j.DeferredCopyingLevel,
				IncMonthsToStore:     j.IncMonthsToStore,
				Sources:              srcs,
				Storages:             sts,
			})
		// "external" as default
		default:

		}
	}

	return
}
