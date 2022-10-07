package mailer

import (
	"io"
	"os/exec"
)

type localMail struct {
	message string
}

func (l localMail) Send(_ string, _ []string, msg io.WriterTo) error {
	_, _ = msg.WriteTo(l)
	cmd := exec.Command("sendmail", "-t", "-oi", l.message)
	return cmd.Run()
}

func (l localMail) Close() error {
	return nil
}

func (l localMail) Write(b []byte) (n int, err error) {
	l.message = string(b)
	return
}
