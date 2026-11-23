package sqlengine

// NAME_TO_PROPERTY maps property keywords to their property kinds; PROPERTY_TO_NAME is the inverse.
var NAME_TO_PROPERTY = map[string]Kind{
	"ALGORITHM":      KAlgorithmProperty,
	"AUTO_INCREMENT": KAutoIncrementProperty,
	"CHARACTER SET":  KCharacterSetProperty,
	"CLUSTERED_BY":   KClusteredByProperty,
	"COLLATE":        KCollateProperty,
	"COMMENT":        KSchemaCommentProperty,
	"CREDENTIALS":    KCredentialsProperty,
	"DEFINER":        KDefinerProperty,
	"DISTKEY":        KDistKeyProperty,
	"DISTRIBUTED_BY": KDistributedByProperty,
	"DISTSTYLE":      KDistStyleProperty,
	"ENGINE":         KEngineProperty,
	"EXECUTE AS":     KExecuteAsProperty,
	"FORMAT":         KFileFormatProperty,
	"LANGUAGE":       KLanguageProperty,
	"LOCATION":       KLocationProperty,
	"LOCK":           KLockProperty,
	"PARTITIONED_BY": KPartitionedByProperty,
	"RETURNS":        KReturnsProperty,
	"ROW_FORMAT":     KRowFormatProperty,
	"SORTKEY":        KSortKeyProperty,
	"ENCODE":         KEncodeProperty,
	"INCLUDE":        KIncludeProperty,
}

var PROPERTY_TO_NAME = map[Kind]string{
	KAlgorithmProperty:     "ALGORITHM",
	KAutoIncrementProperty: "AUTO_INCREMENT",
	KCharacterSetProperty:  "CHARACTER SET",
	KClusteredByProperty:   "CLUSTERED_BY",
	KCollateProperty:       "COLLATE",
	KSchemaCommentProperty: "COMMENT",
	KCredentialsProperty:   "CREDENTIALS",
	KDefinerProperty:       "DEFINER",
	KDistKeyProperty:       "DISTKEY",
	KDistributedByProperty: "DISTRIBUTED_BY",
	KDistStyleProperty:     "DISTSTYLE",
	KEngineProperty:        "ENGINE",
	KExecuteAsProperty:     "EXECUTE AS",
	KFileFormatProperty:    "FORMAT",
	KLanguageProperty:      "LANGUAGE",
	KLocationProperty:      "LOCATION",
	KLockProperty:          "LOCK",
	KPartitionedByProperty: "PARTITIONED_BY",
	KReturnsProperty:       "RETURNS",
	KRowFormatProperty:     "ROW_FORMAT",
	KSortKeyProperty:       "SORTKEY",
	KEncodeProperty:        "ENCODE",
	KIncludeProperty:       "INCLUDE",
}

// Property locations: the positions in a CREATE statement where a property can be rendered.
const (
	Loc_POST_CREATE     = "POST_CREATE"
	Loc_POST_NAME       = "POST_NAME"
	Loc_POST_SCHEMA     = "POST_SCHEMA"
	Loc_POST_WITH       = "POST_WITH"
	Loc_POST_ALIAS      = "POST_ALIAS"
	Loc_POST_EXPRESSION = "POST_EXPRESSION"
	Loc_POST_INDEX      = "POST_INDEX"
	Loc_UNSUPPORTED     = "UNSUPPORTED"
)

var PropertiesLocations = []string{"POST_CREATE", "POST_NAME", "POST_SCHEMA", "POST_WITH", "POST_ALIAS", "POST_EXPRESSION", "POST_INDEX", "UNSUPPORTED"}

// QUERY_MODIFIERS lists the argument names of query modifiers (joins, WHERE, GROUP BY, ORDER BY,
// LIMIT, ...) in clause order.
var QUERY_MODIFIERS = []string{"match", "laterals", "joins", "connect", "pivots", "prewhere", "where", "group", "having", "qualify", "windows", "distribute", "sort", "cluster", "order", "limit", "offset", "locks", "sample", "settings", "format", "options", "for_"}

var (
	TABLE_PARTS  = []string{"this", "db", "catalog"}
	COLUMN_PARTS = []string{"this", "table", "db", "catalog"}
)
