//go:build linux || darwin

package python

const (
	Shell                   = "/bin/sh"
	ShellSubcommandFlag     = "-c"
	VirtualEnvBinaryFolder  = "bin"
	DefaultPythonExecutable = "python3"
)
