package ctx

import (
	"fmt"
	"net/mail"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/hashicorp/go-multierror"
	conf "github.com/nixys/nxs-go-conf"

	"nxs-backup/misc"
)

type confOpts struct {
	ServerName      string              `conf:"server_name" conf_extraopts:"required"`
	Mail            mailConf            `conf:"mail" conf_extraopts:"required"`
	Jobs            []cfgJob            `conf:"jobs"`
	StorageConnects []cfgStorageConnect `conf:"storage_connects"`
	IncludeCfgs     []string            `conf:"include_jobs_configs"`

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
	JobName              string        `conf:"job_name" conf_extraopts:"required"`
	JobType              string        `conf:"type" conf_extraopts:"required"`
	TmpDir               string        `conf:"tmp_dir" conf_extraopts:"required"`
	IncMetadataDir       string        `conf:"inc_metadata_dir"`
	DumpCmd              string        `conf:"dump_cmd"`
	SafetyBackup         bool          `conf:"safety_backup" conf_extraopts:"default=false"`
	DeferredCopyingLevel int           `conf:"deferred_copying_level" conf_extraopts:"default=0"`
	IncMonthsToStore     int           `conf:"inc_months_to_store" conf_extraopts:"default=12"`
	Sources              []cfgSource   `conf:"sources"`
	StoragesOptions      []storageOpts `conf:"storages_options"`
}

type cfgSource struct {
	Name               string `conf:"name" conf_extraopts:"required"`
	Connect            cfgConnect
	SpecialKeys        string   `conf:"special_keys"`
	Targets            []string `conf:"targets"`
	TargetDbs          []string `conf:"target_dbs"`
	TargetCollections  []string `conf:"target_collections"`
	Excludes           []string `conf:"excludes"`
	ExcludeDbs         []string `conf:"exclude_dbs"`
	ExcludeCollections []string `conf:"exclude_collections"`
	Gzip               bool     `conf:"gzip" conf_extraopts:"default=false"`
	SaveAbsPath        bool     `conf:"save_abs_path" conf_extraopts:"default=false"`
	IsSlave            bool     `conf:"is_slave" conf_extraopts:"default=false"`
	ExtraKeys          string   `conf:"extra_keys"`
	SkipBackupRotate   bool     `conf:"skip_backup_rotate" conf_extraopts:"default=false"` // used by external
	PrepareXtrabackup  bool     `conf:"prepare_xtrabackup" conf_extraopts:"default=false"`
}

type cfgConnect struct {
	AuthFile       string        `conf:"auth_file"`
	DBHost         string        `conf:"db_host"`
	DBPort         string        `conf:"db_port"`
	Socket         string        `conf:"socket"`
	SSLMode        string        `conf:"ssl_mode" conf_extraopts:"default=require"`
	DBUser         string        `conf:"db_user"`
	DBPassword     string        `conf:"db_password"`
	PathToConf     string        `conf:"path_to_conf"`
	MongoRSName    string        `conf:"mongo_replica_set_name"`
	MongoRSAddr    string        `conf:"mongo_replica_set_address"`
	ConnectTimeout time.Duration `conf:"connection_timeout" conf_extraopts:"default=10"`
}

type cfgStorageConnect struct {
	Name         string        `conf:"name" conf_extraopts:"required"`
	S3Params     *s3Params     `conf:"s3_params"`
	SftpParams   *sftpParams   `conf:"sftp_params"`
	ScpOptions   *sftpParams   `conf:"scp_params"`
	FtpParams    *ftpParams    `conf:"ftp_params"`
	NfsParams    *nfsParams    `conf:"nfs_params"`
	WebDavParams *webDavParams `conf:"webdav_params"`
	SmbParams    *smbParams    `conf:"smb_params"`
}

type cfgRetention struct {
	Days   int `conf:"days" conf_extraopts:"default=6"`
	Weeks  int `conf:"weeks" conf_extraopts:"default=6"`
	Months int `conf:"months" conf_extraopts:"default=12"`
}

type storageOpts struct {
	StorageName string       `conf:"storage_name" conf_extraopts:"required"`
	BackupPath  string       `conf:"backup_path" conf_extraopts:"required"`
	Retention   cfgRetention `conf:"retention" conf_extraopts:"required"`
}

type s3Params struct {
	BucketName      string `conf:"bucket_name" conf_extraopts:"required"`
	AccessKeyID     string `conf:"access_key_id"`
	SecretAccessKey string `conf:"secret_access_key"`
	Endpoint        string `conf:"endpoint" conf_extraopts:"required"`
	Region          string `conf:"region" conf_extraopts:"required"`
}

type sftpParams struct {
	User           string        `conf:"user" conf_extraopts:"required"`
	Host           string        `conf:"host" conf_extraopts:"required"`
	Port           int           `conf:"port" conf_extraopts:"default=22"`
	Password       string        `conf:"password"`
	KeyFile        string        `conf:"key_file"`
	ConnectTimeout time.Duration `conf:"connection_timeout" conf_extraopts:"default=10"`
}

type ftpParams struct {
	Host              string        `conf:"host"  conf_extraopts:"required"`
	User              string        `conf:"user"`
	Password          string        `conf:"password"`
	Port              int           `conf:"port" conf_extraopts:"default=21"`
	ConnectCount      int           `conf:"connection_count" conf_extraopts:"default=5"`
	ConnectionTimeout time.Duration `conf:"connection_timeout" conf_extraopts:"default=10"`
}

type nfsParams struct {
	Host   string `conf:"host"  conf_extraopts:"required"`
	Target string `conf:"target"`
	UID    uint32 `conf:"uid" conf_extraopts:"default=1000"`
	GID    uint32 `conf:"gid" conf_extraopts:"default=1000"`
	Port   int    `conf:"port" conf_extraopts:"default=111"`
}

type webDavParams struct {
	URL               string        `conf:"url" conf_extraopts:"required"`
	Username          string        `conf:"username"`
	Password          string        `conf:"password"`
	OAuthToken        string        `conf:"oauth_token"`
	ConnectionTimeout time.Duration `conf:"timeout" conf_extraopts:"default=10"`
}

type smbParams struct {
	Host              string        `conf:"host" conf_extraopts:"required"`
	Port              int           `conf:"port" conf_extraopts:"default=445"`
	User              string        `conf:"user" conf_extraopts:"default=Guest"`
	Password          string        `conf:"password"`
	Domain            string        `conf:"domain"`
	Share             string        `conf:"share" conf_extraopts:"required"`
	ConnectionTimeout time.Duration `conf:"timeout" conf_extraopts:"default=10"`
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

	var errs *multierror.Error

	// emails validation
	mailList := c.Mail.ClientMail
	mailList = append(mailList, c.Mail.AdminMail)
	mailList = append(mailList, c.Mail.MailFrom)
	for _, m := range mailList {
		_, err := mail.ParseAddress(m)
		if err != nil {
			errs = multierror.Append(errs, fmt.Errorf("  failed to parse email \"%s\". %s", m, err))
		}
	}

	return errs.ErrorOrNil()
}
