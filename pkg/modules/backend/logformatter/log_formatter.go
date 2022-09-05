package logformatter

import (
	"fmt"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

type BackupLogFormatter struct{}

func (f *BackupLogFormatter) Format(entry *logrus.Entry) ([]byte, error) {

	var (
		out, job, storage string
		s                 []string
	)

	for k, v := range entry.Data {
		switch k {
		case "job":
			job = fmt.Sprintf("%s", v)
		case "storage":
			storage = fmt.Sprintf("%s", v)
		default:
			s = append(s, fmt.Sprintf("%s: %v", k, v))
		}
	}

	out = fmt.Sprintf("[%s]", entry.Time.Format(time.RFC3339Nano))
	if job != "" {
		out += fmt.Sprintf("[%s]", job)
	}
	if storage != "" {
		out += fmt.Sprintf("[%s]", storage)
	}
	out += fmt.Sprintf(" %s: %s", strings.ToUpper(entry.Level.String()), entry.Message)
	if len(s) > 0 {
		out += fmt.Sprintf(" (%s)\n", strings.Join(s, ", "))
	} else {
		out += "\n"
	}

	return []byte(out), nil
}
