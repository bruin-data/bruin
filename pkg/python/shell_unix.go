//go:build linux || darwin

package python

const (
	Shell               = "/bin/sh"
	ShellSubcommandFlag = "-c"
)
