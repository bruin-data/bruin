package sqlengine

// propOwner_X[k] is the class (first in k's MRO) that defines property X, or KNone when
// it is the Expression default.
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
	KUnnest:          KUnnest,
	KDDL:             KDDL,
	KCreate:          KDDL,
	KInsert:          KDDL,
	KSelectable:      KSelectable,
	KDerivedTable:    KDerivedTable,
	KUDTF:            KUDTF,
	KQuery:           KSelectable,
	KCTE:             KDerivedTable,
	KLateral:         KUDTF,
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
