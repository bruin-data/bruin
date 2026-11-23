package sqlengine

// propOwner_X[k] is the kind whose implementation of property X applies to k (k itself or the
// nearest ancestor kind that defines X), or KNone when k uses the default implementation.
var propOwner_name = [numKinds]Kind{
	KStar:          KStar,
	KPlaceholder:   KPlaceholder,
	KNull:          KNull,
	KDot:           KDot,
	KAnonymous:     KAnonymous,
	KOrdered:       KOrdered,
	KDataTypeParam: KDataTypeParam,
	KExecute:       KExecute,
	KExecuteSql:    KExecute,
	KCast:          KCast,
	KTryCast:       KCast,
	KJSONCast:      KCast,
	KFrom:          KFrom,
	KTable:         KTable,
}

var propOwner_output_name = [numKinds]Kind{
	KColumn:            KColumn,
	KLiteral:           KLiteral,
	KPseudocolumn:      KColumn,
	KIdentifier:        KIdentifier,
	KStar:              KStar,
	KDot:               KDot,
	KAlias:             KAlias,
	KPivotAlias:        KAlias,
	KBracket:           KBracket,
	KParen:             KParen,
	KCast:              KCast,
	KTryCast:           KCast,
	KJSONCast:          KCast,
	KJSONExtract:       KJSONExtract,
	KJSONExtractScalar: KJSONExtractScalar,
	KSubquery:          KSubquery,
	KJSONPath:          KJSONPath,
	KTableColumn:       KTableColumn,
}

var propOwner_is_star = [numKinds]Kind{
	KDot:          KDot,
	KSetOperation: KSetOperation,
	KUnion:        KSetOperation,
	KExcept:       KSetOperation,
	KIntersect:    KSetOperation,
	KSelect:       KSelect,
	KSubquery:     KSubquery,
}

var propOwner_alias_or_name = [numKinds]Kind{
	KFrom: KFrom,
	KJoin: KJoin,
}

var propOwner_selects = [numKinds]Kind{
	KGenerator:       KUDTF,
	KExplode:         KUDTF,
	K_ExplodeOuter:   KUDTF,
	KPosexplode:      KUDTF,
	KPosexplodeOuter: KUDTF,
	KSplitToTable:    KUDTF,
	KUnnest:          KUnnest,
	KDDL:             KDDL,
	KCreate:          KDDL,
	KInsert:          KDDL,
	KSelectable:      KSelectable,
	KDerivedTable:    KDerivedTable,
	KUDTF:            KUDTF,
	KQuery:           KSelectable,
	KCTE:             KDerivedTable,
	KLateral:         KLateral,
	KTableFromRows:   KUDTF,
	KTable:           KTable,
	KSetOperation:    KSetOperation,
	KUnion:           KSetOperation,
	KExcept:          KSetOperation,
	KIntersect:       KSetOperation,
	KValues:          KUDTF,
	KSelect:          KSelect,
	KSubquery:        KDerivedTable,
}

var propOwner_named_selects = [numKinds]Kind{
	KGenerator:       KSelectable,
	KExplode:         KSelectable,
	K_ExplodeOuter:   KSelectable,
	KPosexplode:      KSelectable,
	KPosexplodeOuter: KSelectable,
	KSplitToTable:    KSelectable,
	KUnnest:          KSelectable,
	KDDL:             KDDL,
	KCreate:          KDDL,
	KInsert:          KDDL,
	KSelectable:      KSelectable,
	KDerivedTable:    KSelectable,
	KUDTF:            KSelectable,
	KQuery:           KSelectable,
	KCTE:             KSelectable,
	KLateral:         KSelectable,
	KTableFromRows:   KSelectable,
	KTable:           KTable,
	KSetOperation:    KSetOperation,
	KUnion:           KSetOperation,
	KExcept:          KSetOperation,
	KIntersect:       KSetOperation,
	KValues:          KSelectable,
	KSelect:          KSelect,
	KSubquery:        KSelectable,
}

var propOwner_parts = [numKinds]Kind{
	KColumn:       KColumn,
	KPseudocolumn: KColumn,
	KDot:          KDot,
	KTable:        KTable,
}

var propOwner_is_type = [numKinds]Kind{
	KDataType:         KDataType,
	KPseudoType:       KDataType,
	KObjectIdentifier: KDataType,
	KIntervalSpan:     KDataType,
	KCast:             KCast,
	KTryCast:          KCast,
	KJSONCast:         KCast,
}
