package ctx

import (
	"fmt"
	"os"

	appctx "github.com/nixys/nxs-go-appctx/v2"

	"nxs-backup/ctx/args"
	"nxs-backup/interfaces"
	"nxs-backup/modules/backup"
)

// Ctx defines application custom context
type Ctx struct {
	CmdParams interface{}
	Jobs      []interfaces.Job
	Storages  map[string]interfaces.Storage
}

// Init initiates application custom context
func (c *Ctx) Init(opts appctx.CustomContextFuncOpts) (appctx.CfgData, error) {

	// Read config file
	conf, err := confRead(opts.Config)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Set application context
	arg := opts.Args.(*args.Params)
	c.CmdParams = arg.CmdParams

	var errs []error
	c.Jobs, errs = backup.JobsInit(getSettings(conf))
	if len(errs) > 0 {
		fmt.Println("Failed init jobs with next errors:")
		for _, err = range errs {
			fmt.Printf("  %s\n", err)
		}
		os.Exit(1)
	}

	return appctx.CfgData{
		LogFile:  conf.LogFile,
		LogLevel: conf.LogLevel,
		PidFile:  conf.PidFile,
	}, nil
}

// Reload reloads application custom context
func (c *Ctx) Reload(opts appctx.CustomContextFuncOpts) (appctx.CfgData, error) {

	opts.Log.Debug("reloading context")

	return c.Init(opts)
}

// Free frees application custom context
func (c *Ctx) Free(opts appctx.CustomContextFuncOpts) int {

	opts.Log.Debug("freeing context")

	return 0
}
