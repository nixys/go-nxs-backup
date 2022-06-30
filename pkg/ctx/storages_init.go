package ctx

import (
	"fmt"
	"strings"

	"nxs-backup/interfaces"
	"nxs-backup/misc"
	"nxs-backup/modules/storage/ftp"
	"nxs-backup/modules/storage/local"
	"nxs-backup/modules/storage/nfs"
	"nxs-backup/modules/storage/s3"
	"nxs-backup/modules/storage/sftp"
	"nxs-backup/modules/storage/smb"
	"nxs-backup/modules/storage/webdav"
)

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
