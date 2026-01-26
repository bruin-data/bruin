package python

type contextKey string

const (
	LocalIngestr contextKey = "local_ingestr"
	// CtxGongPath is a context key for the gong binary path to use instead of ingestr (when --use-gong).
	CtxGongPath contextKey = "gong_path"
)
