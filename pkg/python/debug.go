package python

type contextKey string

const (
	LocalIngestr contextKey = "local_ingestr"
	// CtxIngestrVersion overrides the default ingestr PyPI version for the asset (e.g. "0.14.2").
	CtxIngestrVersion contextKey = "ingestr_version"
)
