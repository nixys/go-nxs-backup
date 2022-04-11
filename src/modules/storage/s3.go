package storage

type S3 struct {
	BackupPath string
	Retention  Retention
}

func (s S3) IsLocal() int { return 0 }

func (s S3) CopyFile(tmpBackupPath, ofs string, _ bool) (err error) {
	return
}

func (s S3) ListFiles() (err error) {
	return
}

func (s S3) ControlFiles(ofsPartsList []string) (err error) {
	return
}
