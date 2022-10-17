package exec_cmd

import (
	"bytes"
	"os"
	"os/exec"
)

// Result contains command exec Result
type Result struct {
	Stdout   string
	Stderr   string
	ExitCode int
}

// Exec runs command string
func Exec(command string, args ...string) (Result, error) {

	var stderr, stdout bytes.Buffer

	cmd := exec.Command(command, args...)

	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	// Set environment variables
	cmd.Env = os.Environ()

	err := cmd.Run()

	return Result{
		Stdout:   stdout.String(),
		Stderr:   stderr.String(),
		ExitCode: cmd.ProcessState.ExitCode(),
	}, err
}
