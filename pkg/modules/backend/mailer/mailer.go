package mailer

import (
	"fmt"
	"sync"

	appctx "github.com/nixys/nxs-go-appctx/v2"
	"github.com/pkg/errors"
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

func Init(mailCfg MailOpts) (Mailer, error) {
	m := Mailer{opts: mailCfg}

	if !mailCfg.Enabled {
		return m, nil
	}

	if mailCfg.SmtpServer != "" {
		d := gomail.NewDialer(mailCfg.SmtpServer, mailCfg.SmtpPort, mailCfg.SmtpUser, mailCfg.SmtpPassword)
		sc, err := d.Dial()
		defer func() { _ = sc.Close() }()
		if err != nil {
			return Mailer{}, errors.Errorf("Failed to dial SMTP server. Error: %v", err)
		}
	}

	return m, nil
}

// Send sends notification via Email
func (m *Mailer) Send(appCtx *appctx.AppContext, n logger.LogRecord, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	if !m.opts.Enabled || n.Level > m.opts.MessageLevel {
		return
	}

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

	msg.SetBody("text/html", getMailBody(n))

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

func getMailBody(n logger.LogRecord) string {
	return fmt.Sprintf("%v", n)
}
