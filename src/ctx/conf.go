package ctx

import (
	"fmt"
	conf "github.com/nixys/nxs-go-conf"
	"net/mail"
	"os"
	"path"
	"path/filepath"
	"strings"

	"nxs-backup/misc"
)

type confOpts struct {
	ServerName      string   `conf:"server_name" conf_extraopts:"required"`
	Mail            mailConf `conf:"mail" conf_extraopts:"required"`
	Jobs            []Job    `conf:"jobs"`
	IncludeJobsCfgs []string `conf:"include_jobs_configs"`
	LogFile         string   `conf:"logfile" conf_extraopts:"default=stdout"`
	LogLevel        string   `conf:"loglevel" conf_extraopts:"default=info"`
	PidFile         string   `conf:"pidfile"`
	ConfPath        string
	FilesJobs       map[string]Job
	DBsJobs         map[string]Job
	ExtJobs         map[string]Job
}

type mailConf struct {
	SmtpServer   string   `conf:"smtp_server"`
	SmtpPort     int      `conf:"smtp_port"`
	SmtpUser     string   `conf:"smtp_user"`
	SmtpPassword string   `conf:"smtp_password"`
	SmtpTimeout  string   `conf:"smtp_timeout" conf_extraopts:"default=10"`
	AdminMail    string   `conf:"admin_mail"`
	ClientMail   []string `conf:"client_mail"`
	MailFrom     string   `conf:"mail_from"`
	MessageLevel string   `conf:"message_level" conf_extraopts:"default=error"`
}

type Job struct {
	JobName              string    `conf:"job_name" conf_extraopts:"required"`
	JobType              string    `conf:"type" conf_extraopts:"required"`
	TmpDir               string    `conf:"tmp_dir" conf_extraopts:"required"`
	DumpCmd              string    `conf:"dump_cmd"`
	SafetyBackup         bool      `conf:"safety_backup" conf_extraopts:"default=false"`
	DeferredCopyingLevel int       `conf:"deferred_copying_level" conf_extraopts:"default=0"`
	IncMonthsToStore     int       `conf:"inc_months_to_store" conf_extraopts:"default=12"`
	Sources              []source  `conf:"sources"`
	Storages             []storage `conf:"storages"`
}

type source struct {
	Connect            dbConnect
	SpecialKeys        string   `conf:"special_keys"`
	Target             []string `conf:"target"`
	TargetDbs          []string `conf:"target_dbs"`
	TargetCollections  []string `conf:"target_collections"`
	Excludes           []string `conf:"excludes"`
	ExcludeDbs         []string `conf:"exclude_dbs"`
	ExcludeCollections []string `conf:"exclude_collections"`
	Gzip               bool     `conf:"gzip" conf_extraopts:"default=false"`
	SkipBackupRotate   bool     `conf:"skip_backup_rotate" conf_extraopts:"default=false"`
}

type dbConnect struct {
	AuthFile   string `conf:"auth_file"`
	DbHost     string `conf:"db_host"`
	DbPort     string `conf:"db_port"`
	Socket     string `conf:"socket"`
	DbUser     string `conf:"db_user"`
	DbPassword string `conf:"db_password"`
	PathToConf string `conf:"path_to_conf"`
}

type storage struct {
	Storage    string    `conf:"storage" conf_extraopts:"required"`
	Enable     bool      `conf:"enable" conf_extraopts:"default=true"`
	BackupPath string    `conf:"backup_path"`
	Retention  retention `conf:"retention"`
}

type retention struct {
	Days   int `conf:"days"`
	Weeks  int `conf:"weeks"`
	Months int `conf:"months"`
}

func confRead(confPath string) (confOpts, error) {

	var c confOpts

	p, err := misc.PathNormalize(confPath)
	if err != nil {
		return c, err
	}

	err = conf.Load(&c, conf.Settings{
		ConfPath:    p,
		ConfType:    conf.ConfigTypeYAML,
		UnknownDeny: true,
	})
	if err != nil {
		return c, err
	}

	c.ConfPath = confPath

	if len(c.IncludeJobsCfgs) > 0 {
		err := c.jobsRead()
		if err != nil {
			return c, err
		}
	}

	err = c.validate()
	if err != nil {
		fmt.Println("The configuration syntax is incorrect.")
		return c, err
	}

	return c, nil
}

func (c *confOpts) jobsRead() error {

	for _, pathRegexp := range c.IncludeJobsCfgs {
		var p string

		abs, err := filepath.Abs(pathRegexp)
		if err != nil {
			return err
		}
		cp := path.Clean(pathRegexp)
		if abs != cp {
			p = path.Join(path.Dir(c.ConfPath), cp)
		} else {
			p = cp
		}

		err = filepath.Walk(filepath.Dir(p),
			func(fp string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				match, err := path.Match(path.Base(pathRegexp), path.Base(fp))
				if err != nil {
					return err
				}
				if match && !info.IsDir() {
					var j Job

					err = conf.Load(&j, conf.Settings{
						ConfPath:    fp,
						ConfType:    conf.ConfigTypeYAML,
						UnknownDeny: true,
					})
					if err != nil {
						return err
					}

					c.Jobs = append(c.Jobs, j)
				}
				return nil
			})
		if err != nil {
			return err
		}
	}

	return nil
}

// validate checks if provided configuration valid
func (c *confOpts) validate() error {

	var errs []string

	c.FilesJobs = make(map[string]Job)
	c.DBsJobs = make(map[string]Job)
	c.ExtJobs = make(map[string]Job)

	allowedFilesJobTypes := []string{
		"desc_files",
		"inc_files",
	}

	allowedDBsJobTypes := []string{
		"mysql",
		"mysql_xtrabackup",
		"postgresql",
		"postgresql_basebackup",
		"mongodb",
		"redis",
	}

	allowedExtJobTypes := []string{
		"external",
	}

	allowedStorageTypes := []string{
		"local",
		"scp",
		"ftp",
		"smb",
		"nfs",
		"webdav",
		"s3",
	}

	// emails validation
	mailList := c.Mail.ClientMail
	mailList = append(mailList, c.Mail.AdminMail)
	mailList = append(mailList, c.Mail.MailFrom)
	for _, m := range mailList {
		_, err := mail.ParseAddress(m)
		if err != nil {
			errs = append(errs, fmt.Sprintf("  failed to parse email \"%s\". %s", m, err))
		}
	}

	// jobs validation
	for _, j := range c.Jobs {
		if len(j.JobName) == 0 {
			errs = append(errs, fmt.Sprintf("  empty job name is unacceptable"))
		}

		if contains(allowedFilesJobTypes, j.JobType) {
			c.FilesJobs[j.JobName] = j
		} else if contains(allowedDBsJobTypes, j.JobType) {
			c.DBsJobs[j.JobName] = j
		} else if contains(allowedExtJobTypes, j.JobType) {
			c.ExtJobs[j.JobName] = j
		} else {
			errs = append(errs, fmt.Sprintf("  unknown job type \"%s\".", j.JobType))
		}

		for _, s := range j.Storages {
			if !contains(allowedStorageTypes, s.Storage) {
				errs = append(errs, fmt.Sprintf("  unknown storage type \"%s\". Allowd types: %s", s.Storage, strings.Join(allowedStorageTypes, ", ")))
			}

			if s.Retention.Days < 0 || s.Retention.Weeks < 0 || s.Retention.Months < 0 {
				errs = append(errs, fmt.Sprintf("  retention period can't be negative"))
			}
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("Detected next errors:\n%s", strings.Join(errs, "\n"))
	}

	return nil
}

// contains checks if a string is present in a slice
func contains(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}

	return false
}
