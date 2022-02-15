package args

import (
	"fmt"
	"os"

	"github.com/pborman/getopt/v2"
)

type StartOpts struct {
	JobName string
}

// StartRead reads arguments for `start` command
func StartRead(a []string) interface{} {

	args := getopt.New()

	args.SetProgram("nxs-backup start")
	args.SetParameters("job_name")

	args.BoolLong(
		"help",
		'h',
		"Show help")

	args.Parse(a)

	// Show help
	if args.IsSet("help") {
		startHelpPrint(args)
		os.Exit(0)
	}

	if len(a) > 1 {
		return StartOpts{JobName: a[1]}
	}

	return StartOpts{JobName: "all"}
}

func startHelpPrint(args *getopt.Set) {

	additionalDescription := `

Additional description

  Start backup script for one of the job in config file.`

	args.PrintUsage(os.Stdout)
	fmt.Println(additionalDescription)
}
