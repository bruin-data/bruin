//go:build !bruin_no_duckdb

package duck

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/columnar-tech/dbc/config"
	"github.com/stretchr/testify/require"
)

func TestDriverInstallLockAcrossProcesses(t *testing.T) {
	t.Parallel()
	cfg := config.Config{Level: config.ConfigEnv, Location: t.TempDir()}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	type childProcess struct {
		command *exec.Cmd
		output  bytes.Buffer
	}
	children := make([]childProcess, 3)
	err := withDriverInstallLock(ctx, cfg, func() error {
		require.NoError(t, recordDriverTestInstallation(cfg.Location))
		require.NoError(t, os.WriteFile(filepath.Join(cfg.Location, "driver"), []byte("partial"), 0o600))
		for i := range children {
			child := &children[i]
			child.command = exec.CommandContext(ctx, os.Args[0], "-test.run=^TestDriverInstallProcessHelper$", "-test.count=1")
			child.command.Env = append(os.Environ(), "BRUIN_TEST_DRIVER_INSTALL_DIR="+cfg.Location, "BRUIN_TEST_DRIVER_INSTALL_ID="+strconv.Itoa(i))
			child.command.Stdout = &child.output
			child.command.Stderr = &child.output
			require.NoError(t, child.command.Start())
			t.Cleanup(func() {
				if child.command.ProcessState == nil {
					_ = child.command.Process.Kill()
					_ = child.command.Wait()
				}
			})
		}

		// Each child must time out behind this lock before extraction finishes.
		require.Eventually(t, func() bool {
			for i := range children {
				if _, err := os.Stat(filepath.Join(cfg.Location, "waiting-"+strconv.Itoa(i))); err != nil {
					return false
				}
			}
			return true
		}, 10*time.Second, 10*time.Millisecond)
		return os.WriteFile(filepath.Join(cfg.Location, "driver"), []byte("complete"), 0o600)
	})
	require.NoError(t, err)
	for i := range children {
		child := &children[i]
		err := child.command.Wait()
		require.NoError(t, err, "%s", child.output.String())
	}
	installations, err := os.ReadFile(filepath.Join(cfg.Location, "installations"))
	require.NoError(t, err)
	require.Equal(t, "install\n", string(installations), "waiting processes must reuse the completed installation")
}

func TestDriverInstallProcessHelper(t *testing.T) {
	t.Parallel()
	root := os.Getenv("BRUIN_TEST_DRIVER_INSTALL_DIR")
	if root == "" {
		return
	}
	cfg := config.Config{Level: config.ConfigEnv, Location: root}
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	err := withDriverInstallLock(ctx, cfg, func() error {
		return errors.New("entered the installation while another process was extracting")
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NoError(t, os.WriteFile(filepath.Join(root, "waiting-"+os.Getenv("BRUIN_TEST_DRIVER_INSTALL_ID")), nil, 0o600))

	err = withDriverInstallLock(t.Context(), cfg, func() error {
		contents, err := os.ReadFile(filepath.Join(root, "driver"))
		if errors.Is(err, os.ErrNotExist) {
			if err := recordDriverTestInstallation(root); err != nil {
				return err
			}
			return os.WriteFile(filepath.Join(root, "driver"), []byte("complete"), 0o600)
		}
		if err != nil {
			return err
		}
		if string(contents) != "complete" {
			return fmt.Errorf("read an incomplete driver: %q", contents)
		}
		return nil
	})
	require.NoError(t, err)
}

func recordDriverTestInstallation(root string) error {
	file, err := os.OpenFile(filepath.Join(root, "installations"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.WriteString("install\n")
	return err
}

func TestDriverInstallLockReleasesAfterError(t *testing.T) {
	t.Parallel()
	cfg := config.Config{Level: config.ConfigEnv, Location: filepath.Join(t.TempDir(), "adbc")}
	installErr := errors.New("installation failed")
	err := withDriverInstallLock(t.Context(), cfg, func() error { return installErr })
	require.ErrorIs(t, err, installErr)

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	called := false
	err = withDriverInstallLock(ctx, cfg, func() error {
		called = true
		return nil
	})
	require.NoError(t, err)
	require.True(t, called)
}

func TestDriverInstallLockCanceledContext(t *testing.T) {
	t.Parallel()
	cfg := config.Config{Level: config.ConfigEnv, Location: t.TempDir()}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	called := false
	err := withDriverInstallLock(ctx, cfg, func() error {
		called = true
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, called)
}
