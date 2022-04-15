package args

type StartCmd struct {
	JobName string `arg:"positional" placeholder:"JOB GROUP/NAME" default:"all"`
}
