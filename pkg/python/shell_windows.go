//go:build windows
// +build windows

package python

const (
	Shell                   = "cmd"
	ShellSubcommandFlag     = "/c"
	VirtualEnvBinaryFolder  = "Scripts"
	DefaultPythonExecutable = "python"
)
