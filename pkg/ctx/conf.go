package ctx

import (
	"fmt"
	"net/mail"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	conf "github.com/nixys/nxs-go-conf"

	"nxs-backup/interfaces"
	"nxs-backup/misc"
	"nxs-backup/modules/backup"
	"nxs-backup/modules/storage"
	"nxs-backup/modules/storage/ftp"
	"nxs-backup/modules/storage/local"
	"nxs-backup/modules/storage/nfs"
	"nxs-backup/modules/storage/s3"
	"nxs-backup/modules/storage/sftp"
	"nxs-backup/modules/storage/smb"
	"nxs-backup/modules/storage/webdav"
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
	DumpCmd              string        `conf:"dump_cmd"`
	SafetyBackup         bool          `conf:"safety_backup" conf_extraopts:"default=false"`
	DeferredCopyingLevel int           `conf:"deferred_copying_level" conf_extraopts:"default=0"`
	IncMonthsToStore     int           `conf:"inc_months_to_store" conf_extraopts:"default=12"`
	Sources              []cfgSource   `conf:"sources"`
	StoragesOptions      []storageOpts `conf:"storages_options"`
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
	Days   int `conf:"days"`
	Weeks  int `conf:"weeks"`
	Months int `conf:"months"`
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
	User           string `conf:"user" conf_extraopts:"required"`
	Host           string `conf:"host" conf_extraopts:"required"`
	Port           int    `conf:"port" conf_extraopts:"default=22"`
	Password       string `conf:"password"`
	KeyFile        string `conf:"key_file"`
	ConnectTimeout int    `conf:"connection_timeout" conf_extraopts:"default=10"`
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

		var jobStorages []interfaces.Storage

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

		if len(jobStorages) > 1 {
			sort.Sort(interfaces.StorageSortByLocal(jobStorages))
		}

		switch j.JobType {
		case "desc_files":
			var (
				srcs         []backup.DescFilesSource
				ofsPartsList backup.OfsPartsList
			)
			for _, s := range j.Sources {

				var tgts []backup.TargetOfs
				for _, targetPattern := range s.Targets {

					for strings.HasSuffix(targetPattern, "/") {
						targetPattern = strings.TrimSuffix(targetPattern, "/")
					}

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

				srcs = append(srcs, backup.DescFilesSource{
					Targets: tgts,
					Gzip:    s.Gzip,
				})
			}

			jobs = append(jobs, backup.DescFilesJob{
				Name:                 j.JobName,
				TmpDir:               j.TmpDir,
				SafetyBackup:         j.SafetyBackup,
				DeferredCopyingLevel: j.DeferredCopyingLevel,
				Sources:              srcs,
				Storages:             jobStorages,
				NeedToMakeBackup:     needToMakeBackup,
				OfsPartsList:         ofsPartsList,
			})
		// "external" as default
		default:

		}
	}

	return
}

func storagesInit(conf confOpts) (storagesMap map[string]interfaces.Storage, errs []error) {
	var err error
	storagesMap = make(map[string]interfaces.Storage)

	for _, st := range conf.StorageConnects {

		if st.S3Params != nil {

			storagesMap[st.Name], err = s3.Init(s3.Params(*st.S3Params))
			if err != nil {
				errs = append(errs, err)
			}

		} else if st.ScpOptions != nil {

			storagesMap[st.Name], err = sftp.Init(sftp.Params(*st.ScpOptions))
			if err != nil {
				errs = append(errs, err)
			}

		} else if st.SftpParams != nil {

			storagesMap[st.Name], err = sftp.Init(sftp.Params(*st.SftpParams))
			if err != nil {
				errs = append(errs, err)
			}

		} else if st.FtpParams != nil {

			storagesMap[st.Name], err = ftp.Init(ftp.Params(*st.FtpParams))
			if err != nil {
				errs = append(errs, err)
			}

		} else if st.NfsParams != nil {

			storagesMap[st.Name], err = nfs.Init(nfs.Params(*st.NfsParams))
			if err != nil {
				errs = append(errs, err)
			}

		} else if st.WebDavParams != nil {

			storagesMap[st.Name], err = webdav.Init(webdav.Params(*st.WebDavParams))
			if err != nil {
				errs = append(errs, err)
			}

		} else if st.SmbParams != nil {

			storagesMap[st.Name], err = smb.Init(smb.Params(*st.SmbParams))
			if err != nil {
				errs = append(errs, err)
			}

		} else {
			errs = append(errs, fmt.Errorf("unable to define `%s` storage connect type by its params. Allowed connect params: %s", st.Name, strings.Join(misc.AllowedStorageConnectParams, ", ")))
		}
		storagesMap["local"] = local.Init()
	}

	return
}