package ctx

import (
	"errors"
	"fmt"
	"net/mail"

	"github.com/hashicorp/go-multierror"
	"github.com/sirupsen/logrus"

	"nxs-backup/modules/backend/mailer"
)

var messageLevels = map[string]logrus.Level{
	"err":     logrus.ErrorLevel,
	"Err":     logrus.ErrorLevel,
	"ERR":     logrus.ErrorLevel,
	"error":   logrus.ErrorLevel,
	"Error":   logrus.ErrorLevel,
	"ERROR":   logrus.ErrorLevel,
	"warn":    logrus.WarnLevel,
	"Warn":    logrus.WarnLevel,
	"WARN":    logrus.WarnLevel,
	"warning": logrus.WarnLevel,
	"Warning": logrus.WarnLevel,
	"WARNING": logrus.WarnLevel,
	"inf":     logrus.InfoLevel,
	"Inf":     logrus.InfoLevel,
	"INF":     logrus.InfoLevel,
	"info":    logrus.InfoLevel,
	"Info":    logrus.InfoLevel,
	"INFO":    logrus.InfoLevel,
}

func mailerInit(conf confOpts) (mailer.Mailer, error) {
	var errs *multierror.Error

	mailList := conf.Notifications.Mail.Recipients
	for _, m := range mailList {
		_, err := mail.ParseAddress(m)
		if err != nil {
			errs = multierror.Append(errs, fmt.Errorf("  failed to parse email \"%s\". %s", m, err))
		}
	}

	ml, ok := messageLevels[conf.Notifications.Mail.MessageLevel]
	if !ok {
		errs = multierror.Append(errors.New("Unknown Mail message level. Available levels: 'INFO', 'WARN', 'ERR' "))
	}

	m, err := mailer.Init(mailer.MailOpts{
		Enabled:      conf.Notifications.Mail.Enabled,
		SmtpServer:   conf.Notifications.Mail.SmtpServer,
		SmtpPort:     conf.Notifications.Mail.SmtpPort,
		SmtpUser:     conf.Notifications.Mail.SmtpUser,
		SmtpPassword: conf.Notifications.Mail.SmtpPassword,
		Recipients:   conf.Notifications.Mail.Recipients,
		MessageLevel: ml,
		ProjectName:  conf.ProjectName,
		ServerName:   conf.ServerName,
	})
	if err != nil {
		errs = multierror.Append(err)
	}

	return m, errs.ErrorOrNil()
}
