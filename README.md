# Nxs-backup

Nxs-backup is an open source backup software for most popular GNU/Linux distributions. Features of Nxs-backup include
amongst others:

* Support of the most popular storages: local, s3, ssh(sftp), ftp, cifs(smb), nfs, webdav
* Database backups, such as MySQL(logical/physical), PostgreSQL(logical/physical), MongoDB, Redis
* Possibility to specify extra options for collecting database dumps to fine-tune backup process and minimize load on
  the server
* Incremental files backups
* Easy to read and maintain configuration files with clear transparent structure
* Built-in generator of the configuration files to expedite initial setup
* Support of user-defined custom scripts to extend functionality
* Possibility to restore backups with standard tools (no extra software including Nxs-backup is required)
* Email notifications about status and errors during backup process

The source code of Nxs-backup is available at https://github.com/nixys/go-nxs-backup under the license.
Additionally, Nxs-backup offers binary package repositories for the major Linux distributions (Debian, CentOS).

## Getting started

### Understanding Jobs, Type, Sources and Storages

In order to make nxs-backup as ﬂexible as possible, the directions given to nxs-backup are speciﬁed in several pieces.
The main instruction is the job resource, which deﬁnes a job. A backup job generally consists of a Type, a Sources and
Storages.
The Type deﬁnes what type of backup shall run (e.g. MySQL "physical" backups), the Sources defines the target and
exceptions (for each job at least one target must be specified), the Storages define storages where to store backups and
at what quantity (for each job at least one storage must be specified). Work with remote storage is performed by local
mounting of the FS with special tools.

### Setting Up Nxs-backup Conﬁguration Files

Nxs-backup conﬁguration ﬁles are usually located in the */etc/nxs-backup/* directory. The default configuration has only
one configuration file *nxs-backup.conf* and the *conf.d* subdirectory that stores files with descriptions of jobs (one
file per job). Config files are in YAML format. For details, see Settings.

### Generate your Configurations Files for job

You can generate your conﬁguration ﬁle for a job by running the script with the command ***generate*** and *-S*/*
--storages* (list of storages), *-T*/*--type* (type of backup), *-P*/*--path* (path to generated file) options. The
script will generate conﬁguration ﬁle for the job and print result:

 ```bash
# nxs-backup generate -S local scp -T mysql -P /etc/nxs-backup/conf.d/mysql.conf
nxs-backup: Successfully generated '/etc/nxs-backup/conf.d/mysql.conf' configuration file!
```

### Testing your Conﬁguration Files

You can test if conﬁguration is correct by running the script with the ***-t*** option and
optional *-c*/*--config* (path to main conf file). The script will process the conﬁg ﬁle and print any error
messages and then terminate:

```bash
# nxs-backup -t
nxs-backup: The configuration file '/etc/nxs-backup/nxs-backup.conf' syntax is ok!
```

### Start your jobs

You cat start your jobs by running the script with the command ***start*** and optional *-c*/*--config* (path to main
conf file). The script will execute the job passed by the argument. It should be noted that there are several reserved
job names:

+ `all` - simulates the sequential execution of *files*, *databases*, *external* job (default value)
+ `files` - random execution of all jobs with the types *desc_files*, *inc_files*
+ `databases` - random execution of all jobs with the types *mysql*, *mysql_xtrabackup*, *postgresql*, *
  postgresql_basebackup*, *mongodb*, *redis*
+ `external` - random execution of all jobs with the type *external*

```bash
# nxs-backup start all
```

## Settings

### `main`

Nxs-backup main settings block description.

* `server_name`: the name of the server on which the nxs-backup is started
* `project_name`(optional): the name of the project, used for notifications
* `notifications`: contains notification channels parameters
    * `nxs_alert`: nxs-alert notification channel parameters
        * `enabled`: enables notification channel. Default: `false`
        * `auth_key`: nxs-alert auth key.
        * `nxs_alert_url`: URL of the nxs-alert service. Default: `https://nxs-alert.nixys.ru/v2/alert/pool`
        * `message_level`: the level of messages to be notified about. Allowed levels: "debug", "info", "warning", "
          error". Default: `warning`
    * `mail`: contains notification channels parameters
        * `enabled`: enables notification channel. Default: `false`
        * `mail_from`: mailbox on behalf of which mails will be sent
        * `smtp_server`(optional): SMTP host. If not specified email will be sent using `/usr/sbin/sendmail`
        * `smtp_port`(optional): SMTP port
        * `smtp_user`(optional): SMTP user login
        * `smtp_password`(optional): SMTP user password
        * `recipients`: list of notifications recipients emails. Default: `[]`
        * `message_level`: the level of messages to be notified about. Allowed levels: "debug", "info", "warning", "
          error". Default: `warning`
* `storage_connects`: contains list of remote storages connections. Default: `[]`
* `jobs`: contains list of backup jobs. Default: `[]`
* `include_jobs_configs`: contains list of filepaths or glob patterns to job config files. Default: `["conf.d/*.conf"]`
* `logfile`(optional): path to log file. Default: `stdout`
* `waiting_timeout`(optional): time to waite in minutes for another nxs-backup to be completed. Default: disabled

### `storage_connects`

Nxs-backup storage connect settings block description.

* `name`: unique storage name
* `s3_params`(optional): S3 storage type connection parameters
    * `bucket_name`: S3 bucket name
    * `endpoint`: S3 endpoint
    * `region`: S3 region
    * `access_key_id`: S3 access key
    * `secret_access_key`: S3 secret key
* `scp_params`(optional), `sftp_params`(optional): scp/sftp storage type connection parameters
    * `host`: SSH host
    * `port`(optional): SSH port. Default: `22`
    * `user`: SSH user
    * `password`(optional): SSH password
    * `key_file`(optional): path to SSH private key instead of password
    * `connection_timeout`(optional): SSH connection timeout in seconds. Default: `10`
* `ftp_params`(optional): ftp storage type connection parameters
    * `host`: FTP host
    * `port`(optional): FTP port. Default: `21`
    * `user`: FTP user
    * `password`: FTP password
    * `connect_count`(optional): count of FTP connections opens to sever. Default: `5`
    * `connection_timeout`(optional): FTP connection timeout in seconds. Default: `10`
* `nfs_params`(optional): nfs storage type connection parameters
    * `host`: NFS host
    * `port`(optional): NFS port. Default: `111`
    * `target`: path on NFS server where backups will be stored
    * `UID`(optional): UID of NFS server user. Default: `1000`
    * `GID`(optional): GID of NFS server user. Default: `1000`
* `smb_params`(optional): smb storage type connection parameters
    * `host`: SMB host
    * `port`(optional): SMB port. Default: `445`
    * `user`(optional): SMB user. Default: `Guest`
    * `password`(optional): SMB password
    * `share`: SMB share
    * `domain`(optional): SMB domain
    * `connection_timeout`(optional): SMB connection timeout in seconds. Default: `10`
* `webdav_params`(optional): webdav storage type connection parameters
    * `url`: WebDav URL
    * `username`(optional): WebDav user
    * `password`(optional): WebDav password
    * `oauth_token`(optional): WebDav OAuth token
    * `connection_timeout`(optional): WebDav connection timeout in seconds. Default: `10`

### `jobs`

Nxs-backup job settings block description.

* `job`: job name. This value is used to run the required job.
* `type`: type of backup. It can take the following values:
    * *mysql*(MySQL logical backups), *mysql_xtrabackup* (MySQL physical backups), *postgresql*(PostgreSQL logical
      backups), *postgresql_basebackup*(PostgreSQL physical backups), *mongodb*, *redis*
    * *desc_files*, *inc_files*
    * *external*
* `tmp_dir`: a local path to the directory for temporary backups files.
* `safety_backup`(optional)(logical): Delete outdated backups after creating a new one. Default: `false`. **
  IMPORTANT** Using of this option requires more disk space. Perform sure there is enough free space on the end device.
* `deferred_copying`(optional)(logical): Determines that copying of backups to remote storages occurs
  after creation of all temporary backups defined in the task. **IMPORTANT** Using of this option requires more disk
  space for more level. Perform sure there is enough free space on the device where temporary backups stores.
* `sources` (objects array): Specify one target or array of targets for backup:
    * `connect` (object, **Only for *databases* types**). It is necessary to fill a minimum set of keys to allow
      database connection:
        * `db_host`: DB host.
        * `db_port`: DB port.
        * `socket`:  DB socket.
        * `db_user`: DB user.
        * `db_password`: DB password.
        * `auth_file`: DB auth file. You may use either `auth_file` or `db_host` or `socket` options. Options priority
          follows: `auth_file` → `db_host` → `socket`.
        * `path_to_conf`(**only for *mysql_xtrabackup* type**): path to the main mysql configuration file with *client*
          section.
    * `special_keys`(**Only for *databases* types**): special parameters for the collection of database backups
    * `target`: list of databases or directory/files to be backed up. For *databases types* you can use the keyword **
      all** (all db). For *files types* you can use glob patterns.
    * `target_dbs`(**Only for *mongodb* type**): list of mongodb databases to be backed up.
    * `target_collections`(**Only for *mongodb* type**): list of collections of all mongodb databases to be backed up.
      You can use the keyword **all** (all collections in all db).
    * `excludes`: list of databases or directory/files to be excluded from backup. For *files types* you can use glob
      patterns.
    * `exclude_dbs`(**Only for *mongodb* type**):
    * `exclude_collections`(**Only for *mongodb* type**):
    * `gzip`(logicals): compress or not compress the archive
    * `skip_backup_rotate`(**Only for *external* type**)(optional)(logicals): If creation of a local copy is not
      required, for example, in case of copying data to a remote server, rotation of local backups may be skipped with
      this option.
* `storages_options`: specify a list of storages to store backups
    * `storage_name`: name of storage, defined in main config
    * `backup_path`: path to directory for storing backups
    * `retention`: defines retention for backups store
        * `days`: days to store backups. Default: `7`
        * `weeks`: weeks to store backups. Default: `5`
        * `month`: months to store backups. For *inc_files* backup type determines how many months of incremental copies
          will be stored relative to the current month. Can take values from 0 to 12. Default: `12`
* `dump_cmd`: full command to run an external script. **Only for *external* backup type**

## Useful information

### Desc files nxs-backup module

Under the hood there is python module `tarfile`.

### Incremental files nxs-backup module

Under the hood there is python module `tarfile`. Incremental copies of files are made according to the following scheme:
![Incremental backup scheme](https://image.ibb.co/dtLn2p/nxs_inc_backup_scheme_last_version.jpg)

At the beginning of the year or on the first start of the script, a full initial backup is created. Then at the
beginning of each month - an incremental monthly copy from a yearly copy is created. Inside each month there are
incremental ten-day copies. Within each ten-day copy incremental day copies are created.

In this case, since now the tar file is in the PAX format, when you deploy the incremental backup, you do not need to
specify the path to inc-files. All the info is stored in the PAX header of the GNU.dumpdir directory inside the archive.
Therefore, the commands to restore a backup for a specific date are the following:

* First, unpack a full annual copy with the follow command:

```bash
tar xf PATH_TO_FULL_BACKUP
```

* Then alternately unpack the monthly, ten-day, day incremental backups, specifying a special key -G, for example:

```bash
tar xGf PATH_TO_INCREMENTAL_BACKUP
```

### MySQL(logical) nxs-backup module

Under the hood is the work of the `mysqldump`, so for the correct work of the module you must first install **
mysql-client** on the server.

### MySQL(physical) nxs-backup module

Under the hood is the work of the `innobackupex`, so for the correct work of the module you must first install **
percona-xtrabackup** on the server. *Supports only backup of local instance*.

### PostgreSQL(logical) nxs-backup module

The work is based on `pg_dump`, so for the correct work of the module you must first install **postgresql-client** on
the server.

### PostgreSQL(physical) nxs-backup module

The work is based on `pg_basebackup`, so for the correct work of the module you must first install **postgresql-client**
on the server.

### MongoDB nxs-backup module

The work is based on  `mongodump`, so for the correct work of the module you must first install **mongodb-clients** on
the server.

### Redis nxs-backup module

The work is based on  `redis-cli with --rdb option`, so for the correct work of the module you must first install **
redis-tools** on the server.

### External nxs-backup module

In this module, an external script is executed passed to the program via the key "dump_cmd".  
By default at the completion of this command, it is expected that:

* A complete archive of data will be collected
* The stdout will send data in json format, like:

```json
{
  "full_path": "ABS_PATH_TO_ARCHIVE",
  "basename": "BASENAME_ARCHIVE",
  "extension": "EXTENSION_OF_ARCHIVE",
  "gzip": "true|false"
}
```

In this case, the keys basename, extension, gzip are necessary only for the formation of the final name of the backup.
IMPORTANT:

* make sure that there is no unnecessary information in stdout
* *gzip* is a parameter that tells the script whether the file is compressed along the path specified in full_path or
  not, but does not indicate the need for compression at the nxs-backup
* the successfully completed program must exit with 0

If the module was used with the `skip_backup_rotate` parameter, the standard output is expected as a result of running
the command.  
For example, when executing the command "rsync -Pavz /local/source /remote/destination" the result is expected to be a
standard output to stdout.

### SSH storage nxs-backup module

For correct work of the software you must install *openssh-client*, *sshfs*, *sshpass*, *fuse*  packages.

### FTP storage nxs-backup module

For correct work of the software you must install *curlftpfs*, *fuse* packages.

### SMB storage nxs-backup module

For correct work of the software, you must install *cifs-utils*, *fuse* packages.

### NFS storage nxs-backup module

For correct work of the software, you must install *nfs-common*/*nfs-utils*, *fuse* packages.

### WebDAV storage nxs-backup module

For correct work of the software, you must install *davfs2*, *fuse* packages.

### S3 storage nxs-backup module

For correct work of the software, you must install [s3fs](https://github.com/s3fs-fuse/s3fs-fuse)  and *fuse* package.
