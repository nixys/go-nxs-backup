package backup

import (
	"fmt"
	"nxs-backup/misc"
	"path/filepath"
	"sort"

	"nxs-backup/interfaces"
	"nxs-backup/modules/storage"
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
	Targets            []string
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

func JobsInit(js []JobSettings) (jobs []interfaces.Job, errs []error) {

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
			var (
				srcs         []DescFilesSource
				ofsPartsList OfsPartsList
			)
			for _, s := range j.Sources {

				var tgts []TargetOfs
				for _, targetPattern := range s.Targets {

					targetOfsList, err := filepath.Glob(targetPattern)
					if err != nil {
						errs = append(errs, fmt.Errorf("%s. Pattern: %s", err, targetPattern))
						continue
					}

					targetOfsMap := make(map[string]string)
					for _, ofs := range targetOfsList {

						excluded := false
						for _, exclPattern := range s.Excludes {

							match, err := filepath.Match(exclPattern, ofs)
							if err != nil {
								errs = append(errs, fmt.Errorf("%s. Pattern: %s", err, exclPattern))
								continue
							}
							if match {
								excluded = true
								break
							}
						}

						if !excluded {
							ofsPart := misc.GetOfsPart(targetPattern, ofs)
							targetOfsMap[ofsPart] = ofs
							ofsPartsList = append(ofsPartsList, ofsPart)
						}
					}

					tgts = append(tgts, targetOfsMap)
				}

				srcs = append(srcs, DescFilesSource{
					Targets: tgts,
					Gzip:    s.Gzip,
				})
			}

			jobs = append(jobs, DescFilesJob{
				Name:                 j.JobName,
				TmpDir:               j.TmpDir,
				SafetyBackup:         j.SafetyBackup,
				DeferredCopyingLevel: j.DeferredCopyingLevel,
				IncMonthsToStore:     j.IncMonthsToStore,
				Sources:              srcs,
				Storages:             sts,
				NeedToMakeBackup:     len(sts) > 0,
				OfsPartsList:         ofsPartsList,
			})
		// "external" as default
		default:

		}
	}

	return
}
