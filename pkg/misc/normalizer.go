package misc

import (
	"os/user"
	"strings"
)

// DirNormalize normalizes the directory path
func DirNormalize(path string) (string, error) {

	var err error

	path, err = PathNormalize(path)
	if err != nil {
		return path, err
	}

	for strings.HasSuffix(path, "/") {
		path = strings.TrimSuffix(path, "/")
	}
	path += "/"

	return path, nil
}

// PathNormalize normalizes the path
func PathNormalize(path string) (string, error) {

	if strings.HasPrefix(path, "~/") {
		usr, err := user.Current()
		if err != nil {
			return path, err
		}

		path = usr.HomeDir + "/" + strings.TrimPrefix(path, "~/")
	}

	return path, nil
}
