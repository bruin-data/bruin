package python

type contextKey string

const (
	LocalIngestr contextKey = "local_ingestr"
	// CtxGongPath is a context key for the gong binary path to use instead of ingestr (when --use-gong).
	CtxGongPath contextKey = "gong_path"
	// CtxIngestrVersion overrides the bundled ingestr PyPI version for the asset (e.g. "0.14.2").
	CtxIngestrVersion contextKey = "ingestr_version"
)
