package interfaces

type Storage interface {
	CopyFile(tmpBackupPath, ofs string, move bool) error
	ListFiles() error
	ControlFiles() error
	IsLocal() int
}

type SortByLocal []Storage

func (s SortByLocal) Len() int           { return len(s) }
func (s SortByLocal) Less(i, j int) bool { return s[i].IsLocal() < s[j].IsLocal() }
func (s SortByLocal) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
