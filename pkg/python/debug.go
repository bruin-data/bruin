package python

type contextKey string

const (
	LocalIngestr contextKey = "local_ingestr"
	// CtxIngestrVersion overrides the default ingestr release for the asset (e.g. "1.1.18").
	CtxIngestrVersion contextKey = "ingestr_version"
)
