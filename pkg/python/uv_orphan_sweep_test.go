package python

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSweepOrphanArrowTempFiles(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)

	oldArrow := filepath.Join(tmp, tempArrowFilePrefix+"old.arrow")
	oldScript := filepath.Join(tmp, tempArrowScriptPrefix+"old.py")
	freshArrow := filepath.Join(tmp, tempArrowFilePrefix+"fresh.arrow")
	freshScript := filepath.Join(tmp, tempArrowScriptPrefix+"fresh.py")
	unrelated := filepath.Join(tmp, "unrelated.arrow")

	for _, p := range []string{oldArrow, oldScript, freshArrow, freshScript, unrelated} {
		require.NoError(t, os.WriteFile(p, []byte("x"), 0o600))
	}

	pastMtime := time.Now().Add(-2 * orphanTempFileMaxAge)
	require.NoError(t, os.Chtimes(oldArrow, pastMtime, pastMtime))
	require.NoError(t, os.Chtimes(oldScript, pastMtime, pastMtime))
	require.NoError(t, os.Chtimes(unrelated, pastMtime, pastMtime))

	sweepOrphanArrowTempFiles()

	require.NoFileExists(t, oldArrow, "old arrow file should be swept")
	require.NoFileExists(t, oldScript, "old script file should be swept")
	require.FileExists(t, freshArrow, "recent arrow file must be preserved")
	require.FileExists(t, freshScript, "recent script file must be preserved")
	require.FileExists(t, unrelated, "files not matching the prefix must be preserved")
}
