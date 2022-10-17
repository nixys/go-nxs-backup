package notifier

import (
	"fmt"
	"io"
	"os/exec"
	"sync"

	appctx "github.com/nixys/nxs-go-appctx/v2"
	"github.com/sirupsen/logrus"
	"gopkg.in/gomail.v2"

	"nxs-backup/modules/logger"
)

type MailOpts struct {
	Enabled      bool
	SmtpServer   string
	SmtpPort     int
	SmtpUser     string
	SmtpPassword string
	SmtpTimeout  string
	Recipients   []string
	MessageLevel logrus.Level
	ProjectName  string
	ServerName   string
}

type Mailer struct {
	opts MailOpts
}

func MailerInit(mailCfg MailOpts) (Mailer, error) {
	m := Mailer{opts: mailCfg}

	if !mailCfg.Enabled {
		return m, nil
	}

	if mailCfg.SmtpServer != "" {
		d := gomail.NewDialer(mailCfg.SmtpServer, mailCfg.SmtpPort, mailCfg.SmtpUser, mailCfg.SmtpPassword)
		sc, err := d.Dial()
		if err != nil {
			return Mailer{}, fmt.Errorf("Failed to dial SMTP server. Error: %v ", err)
		}
		defer func() { _ = sc.Close() }()
	}

	return m, nil
}

// Send sends notification via Email
func (m *Mailer) Send(appCtx *appctx.AppContext, n logger.LogRecord, wg *sync.WaitGroup) {
	if !m.opts.Enabled || n.Level > m.opts.MessageLevel {
		return
	}

	wg.Add(1)
	defer wg.Done()

	var (
		sc  gomail.SendCloser
		err error
	)
	defer func() { _ = sc.Close() }()

	msg := gomail.NewMessage()
	msg.SetHeader("From", m.opts.SmtpUser)
	msg.SetHeader("To", m.opts.Recipients...)

	subjStr := fmt.Sprintf("[%s] Nxs-backup notification: server %q", n.Level, m.opts.ServerName)
	if m.opts.ProjectName != "" {
		subjStr += fmt.Sprintf(" of project %q", m.opts.ProjectName)
	}
	msg.SetHeader("Subject", subjStr)

	msg.SetBody("text/html", m.getMailBody(n))

	if m.opts.SmtpServer != "" {
		d := gomail.NewDialer(m.opts.SmtpServer, m.opts.SmtpPort, m.opts.SmtpUser, m.opts.SmtpPassword)
		sc, err = d.Dial()
		if err != nil {
			appCtx.Log().Errorf("Failed to dial SMTP server. Error: %v", err)
			return
		}
	} else {
		sc = localMail{}
	}

	if err = gomail.Send(sc, msg); err != nil {
		appCtx.Log().Errorf("Could not send email: %v", err)
	}
}

func (m *Mailer) getMailBody(n logger.LogRecord) (b string) {
	switch n.Level {
	case logrus.DebugLevel:
		b += fmt.Sprint("[DEBUG]:\n\n")
	case logrus.InfoLevel:
		b += fmt.Sprint("[INFO]:\n\n")
	case logrus.WarnLevel:
		b += fmt.Sprint("[WARNING]:\n\n")
	case logrus.ErrorLevel:
		b += fmt.Sprint("[ERROR]:\n\n")
	}

	if m.opts.ProjectName != "" {
		b += fmt.Sprintf("Project: %s\n", m.opts.ProjectName)
	}
	if m.opts.ServerName != "" {
		b += fmt.Sprintf("Server: %s\n\n", m.opts.ServerName)
	}

	if n.JobName != "" {
		b += fmt.Sprintf("Job: %s\n", n.JobName)
	}
	if n.StorageName != "" {
		b += fmt.Sprintf("Storage: %s\n", n.StorageName)
	}
	b += fmt.Sprintf("Message: %s\n", n.Message)

	return
}

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
