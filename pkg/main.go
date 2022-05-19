package main

import (
	"fmt"
	"nxs-backup/modules/cmd"
	"os"
	"syscall"

	appctx "github.com/nixys/nxs-go-appctx/v2"
	"nxs-backup/ctx"
	"nxs-backup/ctx/args"
)

func main() {

	subCmds := args.SubCmds{
		"start":   cmd.Start,
		"testCfg": cmd.TestConfig,
	}

	// Read command line arguments
	a := args.Read(subCmds)

	// Init appctx
	appCtx, err := appctx.ContextInit(appctx.Settings{
		CustomContext:    &ctx.Ctx{},
		Args:             &a,
		CfgPath:          a.ConfigPath,
		TermSignals:      []os.Signal{syscall.SIGTERM, syscall.SIGINT},
		ReloadSignals:    []os.Signal{syscall.SIGHUP},
		LogrotateSignals: []os.Signal{syscall.SIGUSR1},
		//LogFormatter:     &logrus.JSONFormatter{},
		//LogFormatter: &logrus.TextFormatter{},
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
