package ctx

import (
	"github.com/alexflint/go-arg"
	appctx "github.com/nixys/nxs-go-appctx/v2"

	"nxs-backup/misc"
)

type cmdHandler func(*appctx.AppContext) error

// SubCmds contains Command name and handler
type SubCmds map[string]cmdHandler

// ArgsParams contains parameters read from command line, command parameters and command handler
type ArgsParams struct {
	ConfigPath string
	CmdHandler cmdHandler
	CmdParams  interface{}
}

type StartCmd struct {
	JobName string `arg:"positional" placeholder:"JOB GROUP/NAME" default:"all"`
}

type args struct {
	Start    *StartCmd `arg:"subcommand:start"`
	ConfPath string    `arg:"-c,--config" help:"Path to config file" default:"/etc/nxs-backup/nxs-backup.conf" placeholder:"PATH"`
	TestConf bool      `arg:"-t,--test-config" help:"Check if configuration syntax correct"`
}

// ReadArgs reads arguments from command line
func ReadArgs(cmds SubCmds) (p ArgsParams) {

	var a args

	curArgs := arg.MustParse(&a)

	p.ConfigPath = a.ConfPath

	if a.TestConf {
		p.CmdHandler = cmds["testCfg"]
		return
	}

	p.CmdParams = curArgs.Subcommand()
	p.CmdHandler = cmds[curArgs.SubcommandNames()[0]]

	return p
}

func (args) Version() string {
	return "nxs-backup " + misc.VERSION
}
