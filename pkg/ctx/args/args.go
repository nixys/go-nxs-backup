package args

import (
	"github.com/alexflint/go-arg"
	appctx "github.com/nixys/nxs-go-appctx/v2"

	"nxs-backup/misc"
)

type cmdHandler func(*appctx.AppContext) error

// SubCmds contains Command name and handler
type SubCmds map[string]cmdHandler

// Params contains parameters read from command line, command parameters and command handler
type Params struct {
	ConfigPath string
	CmdHandler cmdHandler
	CmdParams  interface{}
}

type Args struct {
	Start    *StartCmd `arg:"subcommand:start"`
	ConfPath string    `arg:"-c,--config" help:"Path to config file" default:"/etc/nxs-backup/nxs-backup.conf" placeholder:"PATH"`
	TestConf bool      `arg:"-t" help:"Check if configuration syntax correct"`
}

// Read reads arguments from command line
func Read(cmds SubCmds) (p Params) {

	var a Args

	args := arg.MustParse(&a)

	p.ConfigPath = a.ConfPath

	if a.TestConf {
		p.CmdHandler = cmds["testCfg"]
		return
	}

	p.CmdParams = args.Subcommand()
	p.CmdHandler = cmds[args.SubcommandNames()[0]]

	return p
}

func (Args) Version() string {
	return "nxs-backup " + misc.VERSION
}
