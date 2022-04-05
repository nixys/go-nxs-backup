package main

import (
	"fmt"
	"os"
	"syscall"

	appctx "github.com/nixys/nxs-go-appctx/v2"
	"github.com/sirupsen/logrus"

	"nxs-backup/ctx"
	"nxs-backup/ctx/args"
	"nxs-backup/modules/cmd"
)

func main() {

	commands := []args.Command{
		{
			Cmd:         "start",
			ArgsHandler: args.StartRead,
			CmdHandler:  cmd.Start,
		},
	}

	// Read command line arguments
	a := args.Read(commands)

	// Init appctx
	appCtx, err := appctx.ContextInit(appctx.Settings{
		CustomContext:    &ctx.Ctx{},
		Args:             &a,
		CfgPath:          a.ConfigPath,
		TermSignals:      []os.Signal{syscall.SIGTERM, syscall.SIGINT},
		ReloadSignals:    []os.Signal{syscall.SIGHUP},
		LogrotateSignals: []os.Signal{syscall.SIGUSR1},
		LogFormatter:     &logrus.JSONFormatter{},
	})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// exec found command
	if err := a.CmdHandler(appCtx); err != nil {
		fmt.Println("exec error: ", err)
		os.Exit(1)
	}

	os.Exit(0)
}
