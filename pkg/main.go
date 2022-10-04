package main

import (
	"context"
	"fmt"
	"os"
	"syscall"

	appctx "github.com/nixys/nxs-go-appctx/v2"

	"nxs-backup/ctx"
	"nxs-backup/modules/cmd"
	"nxs-backup/modules/logger"
	"nxs-backup/routines/logging"
)

func main() {

	subCmds := ctx.SubCmds{
		"start":   cmd.Start,
		"testCfg": cmd.TestConfig,
	}

	// Read command line arguments
	a := ctx.ReadArgs(subCmds)

	// Init appctx
	appCtx, err := appctx.ContextInit(appctx.Settings{
		CustomContext:    &ctx.Ctx{},
		Args:             &a,
		CfgPath:          a.ConfigPath,
		TermSignals:      []os.Signal{syscall.SIGTERM, syscall.SIGINT},
		ReloadSignals:    []os.Signal{syscall.SIGHUP},
		LogrotateSignals: []os.Signal{syscall.SIGUSR1},
		LogFormatter:     &logger.LogFormatter{},
	})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Create main context
	c := context.Background()
	// Create notifications routine
	appCtx.RoutineCreate(c, logging.Runtime)

	// exec found command
	if err = a.CmdHandler(appCtx); err != nil {
		fmt.Println("exec error: ", err)
		os.Exit(1)
	}

	os.Exit(0)
}
