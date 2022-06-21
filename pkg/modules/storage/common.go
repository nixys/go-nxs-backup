package storage

import (
	"errors"
)

var (
	ErrorObjectNotFound = errors.New("object not found")
	ErrorFileNotFound   = errors.New("file does not exist")
)

type Retention struct {
	Days   int
	Weeks  int
	Months int
}
