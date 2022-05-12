package ctx

import (
	"fmt"
	"net/mail"
	"os"
	"path"
	"path/filepath"
	"strings"

	conf "github.com/nixys/nxs-go-conf"

	"nxs-backup/misc"
	"nxs-backup/modules/backup"
)

type confOpts struct {
	ServerName  string       `conf:"server_name" conf_extraopts:"required"`
	Mail        mailConf     `conf:"mail" conf_extraopts:"required"`
	Jobs        []cfgJob     `conf:"jobs"`
	Storages    []cfgStorage `conf:"storages"`
	IncludeCfgs []string     `conf:"include_jobs_configs"`

	LogFile  string `conf:"logfile" conf_extraopts:"default=stdout"`
	LogLevel string `conf:"loglevel" conf_extraopts:"default=info"`
	PidFile  string `conf:"pidfile"`
	ConfPath string
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

type cfgJob struct {
	JobName              string       `conf:"job_name" conf_extraopts:"required"`
	JobType              string       `conf:"type" conf_extraopts:"required"`
	TmpDir               string       `conf:"tmp_dir" conf_extraopts:"required"`
	DumpCmd              string       `conf:"dump_cmd"`
	SafetyBackup         bool         `conf:"safety_backup" conf_extraopts:"default=false"`
	DeferredCopyingLevel int          `conf:"deferred_copying_level" conf_extraopts:"default=0"`
	IncMonthsToStore     int          `conf:"inc_months_to_store" conf_extraopts:"default=12"`
	Sources              []cfgSource  `conf:"sources"`
	Storages             []cfgStorage `conf:"storages"`
	StorageNames         []string     `conf:"storage_names"`
}

type cfgSource struct {
	Connect            cfgConnect
	SpecialKeys        string   `conf:"special_keys"`
	Targets            []string `conf:"targets"`
	TargetDbs          []string `conf:"target_dbs"`
	TargetCollections  []string `conf:"target_collections"`
	Excludes           []string `conf:"excludes"`
	ExcludeDbs         []string `conf:"exclude_dbs"`
	ExcludeCollections []string `conf:"exclude_collections"`
	Gzip               bool     `conf:"gzip" conf_extraopts:"default=false"`
	SkipBackupRotate   bool     `conf:"skip_backup_rotate" conf_extraopts:"default=false"` // used by external
}

type cfgConnect struct {
	AuthFile   string `conf:"auth_file"`
	DBHost     string `conf:"db_host"`
	DBPort     string `conf:"db_port"`
	Socket     string `conf:"socket"`
	DBUser     string `conf:"db_user"`
	DBPassword string `conf:"db_password"`
	PathToConf string `conf:"path_to_conf"`
}

type cfgStorage struct {
	Name         string        `conf:"name" conf_extraopts:"required"`
	Enable       bool          `conf:"enable" conf_extraopts:"default=true"`
	Retention    cfgRetention  `conf:"retention" conf_extraopts:"required"`
	LocalOptions *localOptions `conf:"local_options"`
	S3Options    *s3Options    `conf:"s3_options"`
	SFTPOptions  *sftpOptions  `conf:"sftp_options"`
	SCPOptions   *sftpOptions  `conf:"scp_options"`
}

type cfgRetention struct {
	Days   int `conf:"days"`
	Weeks  int `conf:"weeks"`
	Months int `conf:"months"`
}

type localOptions struct {
	BackupPath string `conf:"backup_path" conf_extraopts:"required"`
}

type s3Options struct {
	BackupPath      string `conf:"backup_path" conf_extraopts:"required"`
	BucketName      string `conf:"bucket_name" conf_extraopts:"required"`
	AccessKeyID     string `conf:"access_key_id"`
	SecretAccessKey string `conf:"secret_access_key"`
	Endpoint        string `conf:"endpoint" conf_extraopts:"required"`
	Region          string `conf:"region" conf_extraopts:"required"`
}

type sftpOptions struct {
	BackupPath     string `conf:"backup_path" conf_extraopts:"required"`
	User           string `conf:"user" conf_extraopts:"required"`
	Host           string `conf:"host" conf_extraopts:"required"`
	Port           int    `conf:"port" conf_extraopts:"default=22"`
	Password       string `conf:"password"`
	KeyFile        string `conf:"key_file"`
	ConnectTimeout int    `conf:"connection_timeout" conf_extraopts:"default=60"`
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

	if len(c.IncludeCfgs) > 0 {
		err = c.extraCfgsRead()
		if err != nil {
			fmt.Println("Configuration cannot be read.")
			return c, err
		}
	}

	err = c.validate()
	if err != nil {
		fmt.Println("The configuration is incorrect.")
		return c, err
	}

	return c, nil
}

func (c *confOpts) extraCfgsRead() error {

	for _, pathRegexp := range c.IncludeCfgs {
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
					var j cfgJob

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

	if len(errs) > 0 {
		return fmt.Errorf("Detected next errors:\n%s", strings.Join(errs, "\n"))
	}

	return nil
}

func getSettings(conf confOpts) (jobs []backup.JobSettings, sts map[string]backup.StorageSettings) {

	sts = make(map[string]backup.StorageSettings)

	for _, j := range conf.Jobs {

		var srcs []backup.SourceSettings
		var sns []string

		for _, src := range j.Sources {
			srcs = append(srcs, backup.SourceSettings{
				Targets:            src.Targets,
				TargetDbs:          src.TargetDbs,
				TargetCollections:  src.TargetCollections,
				Excludes:           src.Excludes,
				ExcludeDbs:         src.ExcludeDbs,
				ExcludeCollections: src.ExcludeCollections,
				SpecialKeys:        src.SpecialKeys,
				Gzip:               src.Gzip,
				SkipBackupRotate:   src.SkipBackupRotate,
				ConnectSettings: backup.ConnectSettings{
					Socket:     src.Connect.Socket,
					AuthFile:   src.Connect.AuthFile,
					DBHost:     src.Connect.DBHost,
					DBPort:     src.Connect.DBPort,
					DBUser:     src.Connect.DBUser,
					DBPassword: src.Connect.DBPassword,
					PathToConf: src.Connect.PathToConf,
				},
			})
		}

		for _, st := range j.Storages {

			ss := backup.StorageSettings{
				Enable:    st.Enable,
				Retention: backup.RetentionSettings(st.Retention),
			}
			if st.S3Options != nil {
				ss.Type = "s3"
				ss.S3Options = backup.S3Options(*st.S3Options)
			} else if st.SCPOptions != nil {
				ss.Type = "scp"
				ss.SFTPOptions = backup.SFTPOptions(*st.SCPOptions)
			} else if st.SFTPOptions != nil {
				ss.Type = "sftp"
				ss.SFTPOptions = backup.SFTPOptions(*st.SFTPOptions)
			} else if st.LocalOptions != nil {
				ss.Type = "local"
				ss.LocalOptions = backup.LocalOptions(*st.LocalOptions)
			}
			sts[st.Name] = ss
			sns = append(sns, st.Name)
		}

		jobs = append(jobs, backup.JobSettings{
			JobName:              j.JobName,
			JobType:              j.JobType,
			TmpDir:               j.TmpDir,
			DumpCmd:              j.DumpCmd,
			SafetyBackup:         j.SafetyBackup,
			DeferredCopyingLevel: j.DeferredCopyingLevel,
			IncMonthsToStore:     j.IncMonthsToStore,
			Sources:              srcs,
			StorageNames:         append(sns, j.StorageNames...),
		})
	}

	return
}
