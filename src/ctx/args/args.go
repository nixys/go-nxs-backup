package args

import (
	"fmt"
	"nxs-backup/misc"
	"os"
	"strings"

	appctx "github.com/nixys/nxs-go-appctx/v2"
	"github.com/pborman/getopt/v2"
)

const (
	confPathDefault = "/etc/nxs-backup/nxs-backup.conf"
)

type argsHandler func([]string) interface{}
type cmdHandler func(*appctx.AppContext) error

// Command contains Command with subcommands and handlers
type Command struct {
	Cmd         string
	SubCmds     []Command
	CmdHandler  cmdHandler
	ArgsHandler argsHandler
}

// Args contains arguments value read from command line
type Args struct {
	ConfigPath string
	CmdHandler cmdHandler
	Values     interface{}
}

// Read reads arguments from command line
func Read(commands []Command) Args {

	var a Args

	args := getopt.New()

	args.SetParameters("[cmd ...]")

	args.BoolLong(
		"help",
		'h',
		"Show help")

	args.BoolLong(
		"version",
		'v',
		"Show program version")

	confPath := args.StringLong(
		"conf",
		'c',
		confPathDefault,
		"Config file path")

	args.Parse(os.Args)

	a.ConfigPath = *confPath

	// Show help
	if args.IsSet("help") {
		helpPrint(args)
		os.Exit(0)
	}

	// Show version
	if args.IsSet("version") {
		argsVersionPrint()
		os.Exit(0)
	}

	// Lookup command
	c, tail := commandLookup(commands, args.Args())

	subArgs := []string{os.Args[0]}
	subArgs = append(subArgs, tail...)

	// If command was not found
	if c.ArgsHandler == nil {
		fmt.Println("Unknown command: ", tail)
		helpPrint(args)
		os.Exit(1)
	}
	// If command handler was not found
	if c.CmdHandler == nil {
		fmt.Println("empty command handler")
		os.Exit(1)
	}
	a.CmdHandler = c.CmdHandler
	a.Values = c.ArgsHandler(subArgs)

	return a
}

// commandLookup looks up ArgsCommand by cmds slice specified in args
func commandLookup(commands []Command, cmds []string) (Command, []string) {

	if len(cmds) == 0 {
		return Command{}, cmds
	}

	t := cmds[0]

	for _, c := range commands {
		if c.Cmd == t {

			if len(cmds) == 1 {
				return c, []string{}
			}

			if strings.HasPrefix(cmds[1], "-") {
				return c, cmds[1:]
			}

			if len(c.SubCmds) == 0 {
				return c, cmds[1:]
			}

			return commandLookup(c.SubCmds, cmds[1:])
		}
	}

	return Command{}, cmds
}

func helpPrint(args *getopt.Set) {

	additionalDescription := `

Additional description

  Command line tools for nixy-server.

  Following cmd are available (use autocomplete):

	- project
	- script`

	args.PrintUsage(os.Stdout)
	fmt.Println(additionalDescription)
}

func argsVersionPrint() {
	fmt.Println(misc.VERSION)
}
