package sqlengine

// generatorHooks holds generator methods that dialects override (virtual dispatch).
// Base implementations are named base<Method>; call sites use the wrapper methods.
type generatorHooks struct {
	init                                   func(g *Generator)
	generate                               func(g *Generator, e *Expr, copy bool) string
	jsonpathkeySQL                         func(g *Generator, expression *Expr) string
	jsonpathsubscriptSQL                   func(g *Generator, expression *Expr) string
	addColumnSQL                           func(g *Generator, expression *Expr) string
	afterLimitModifiers                    func(g *Generator, expression *Expr) []string
	aliasesSQL                             func(g *Generator, expression *Expr) string
	alterSQL                               func(g *Generator, expression *Expr) string
	altercolumnSQL                         func(g *Generator, expression *Expr) string
	alterrenameSQL                         func(g *Generator, expression *Expr, includeTo bool) string
	altersetSQL                            func(g *Generator, expression *Expr) string
	anyvalueSQL                            func(g *Generator, expression *Expr) string
	arrayaggSQL                            func(g *Generator, expression *Expr) string
	attimezoneSQL                          func(g *Generator, expression *Expr) string
	autoincrementcolumnconstraintSQL       func(g *Generator, expression *Expr) string
	bitwisenotSQL                          func(g *Generator, expression *Expr) string
	bitwisexorSQL                          func(g *Generator, expression *Expr) string
	booleanSQL                             func(g *Generator, expression *Expr) string
	bracketSQL                             func(g *Generator, expression *Expr) string
	castSQL                                func(g *Generator, expression *Expr, safePrefix string) string
	chrSQL                                 func(g *Generator, expression *Expr, name string) string
	clusterpropertySQL                     func(g *Generator, expression *Expr) string
	collateSQL                             func(g *Generator, expression *Expr) string
	columnParts                            func(g *Generator, expression *Expr) string
	columndefSQL                           func(g *Generator, expression *Expr, sep string) string
	commitSQL                              func(g *Generator, expression *Expr) string
	computedcolumnconstraintSQL            func(g *Generator, expression *Expr) string
	concatwsSQL                            func(g *Generator, expression *Expr) string
	constraintSQL                          func(g *Generator, expression *Expr) string
	convertSQL                             func(g *Generator, expression *Expr) string
	converttimezoneSQL                     func(g *Generator, expression *Expr) string
	createSQL                              func(g *Generator, expression *Expr) string
	createableSQL                          func(g *Generator, expression *Expr, locations propLocations) string
	cteSQL                                 func(g *Generator, expression *Expr) string
	currentdateSQL                         func(g *Generator, expression *Expr) string
	datatypeSQL                            func(g *Generator, expression *Expr) string
	deleteSQL                              func(g *Generator, expression *Expr) string
	describeSQL                            func(g *Generator, expression *Expr) string
	dotSQL                                 func(g *Generator, expression *Expr) string
	dpipeSQL                               func(g *Generator, expression *Expr) string
	dropSQL                                func(g *Generator, expression *Expr) string
	dynamicidentifierSQL                   func(g *Generator, expression *Expr) string
	eqSQL                                  func(g *Generator, expression *Expr) string
	executeSQL                             func(g *Generator, expression *Expr) string
	executesqlSQL                          func(g *Generator, expression *Expr) string
	existsSQL                              func(g *Generator, expression *Expr) string
	extractSQL                             func(g *Generator, expression *Expr) string
	filterSQL                              func(g *Generator, expression *Expr) string
	formatTime                             func(g *Generator, expression *Expr, inverseTimeMapping map[string]string, inverseTimeTrie *trie) string
	generatedasidentitycolumnconstraintSQL func(g *Generator, expression *Expr) string
	hexSQL                                 func(g *Generator, expression *Expr) string
	hexstringSQL                           func(g *Generator, expression *Expr, binaryFunctionRepr string) string
	hintSQL                                func(g *Generator, expression *Expr) string
	identifierSQL                          func(g *Generator, expression *Expr) string
	ifblockSQL                             func(g *Generator, expression *Expr) string
	ignorenullsSQL                         func(g *Generator, expression *Expr) string
	inSQL                                  func(g *Generator, expression *Expr) string
	inUnnestOp                             func(g *Generator, unnest *Expr) string
	indexcolumnconstraintSQL               func(g *Generator, expression *Expr) string
	installSQL                             func(g *Generator, expression *Expr) string
	intervalSQL                            func(g *Generator, expression *Expr) string
	intoSQL                                func(g *Generator, expression *Expr) string
	isSQL                                  func(g *Generator, expression *Expr) string
	joinSQL                                func(g *Generator, expression *Expr) string
	jsonpathSQL                            func(g *Generator, expression *Expr) string
	lambdaSQL                              func(g *Generator, expression *Expr, arrowSep string, wrap bool) string
	lateralOp                              func(g *Generator, expression *Expr) string
	lateralSQL                             func(g *Generator, expression *Expr) string
	likepropertySQL                        func(g *Generator, expression *Expr) string
	locateProperties                       func(g *Generator, properties *Expr) propLocations
	logSQL                                 func(g *Generator, expression *Expr) string
	matchagainstSQL                        func(g *Generator, expression *Expr) string
	modSQL                                 func(g *Generator, expression *Expr) string
	modelattributeSQL                      func(g *Generator, expression *Expr) string
	neqSQL                                 func(g *Generator, expression *Expr) string
	notSQL                                 func(g *Generator, expression *Expr) string
	offsetLimitModifiers                   func(g *Generator, expression *Expr, fetch bool, limit *Expr) []string
	offsetSQL                              func(g *Generator, expression *Expr) string
	onclusterSQL                           func(g *Generator, expression *Expr) string
	optionsModifier                        func(g *Generator, expression *Expr) string
	padSQL                                 func(g *Generator, expression *Expr) string
	parameterSQL                           func(g *Generator, expression *Expr) string
	parsedatetimeSQL                       func(g *Generator, expression *Expr) string
	parsejsonSQL                           func(g *Generator, expression *Expr) string
	partitionSQL                           func(g *Generator, expression *Expr) string
	partitionbyrangepropertySQL            func(g *Generator, expression *Expr) string
	partitionbyrangepropertydynamicSQL     func(g *Generator, expression *Expr) string
	partitionrangeSQL                      func(g *Generator, expression *Expr) string
	placeholderSQL                         func(g *Generator, expression *Expr) string
	prewhereSQL                            func(g *Generator, expression *Expr) string
	queryoptionSQL                         func(g *Generator, expression *Expr) string
	randSQL                                func(g *Generator, expression *Expr) string
	refreshtriggerpropertySQL              func(g *Generator, expression *Expr) string
	renamecolumnSQL                        func(g *Generator, expression *Expr) string
	respectnullsSQL                        func(g *Generator, expression *Expr) string
	returningSQL                           func(g *Generator, expression *Expr) string
	rollbackSQL                            func(g *Generator, expression *Expr) string
	schemaSQL                              func(g *Generator, expression *Expr) string
	scopeResolution                        func(g *Generator, rhs string, scopeName string) string
	selectSQL                              func(g *Generator, expression *Expr) string
	setitemSQL                             func(g *Generator, expression *Expr) string
	showSQL                                func(g *Generator, expression *Expr) string
	spaceSQL                               func(g *Generator, expression *Expr) string
	storedprocedureSQL                     func(g *Generator, expression *Expr) string
	strtodateSQL                           func(g *Generator, expression *Expr) string
	strtotimeSQL                           func(g *Generator, expression *Expr) string
	structSQL                              func(g *Generator, expression *Expr) string
	tableParts                             func(g *Generator, expression *Expr) string
	tableSQL                               func(g *Generator, expression *Expr, sep string) string
	tablefromrowsSQL                       func(g *Generator, expression *Expr) string
	tablesampleSQL                         func(g *Generator, expression *Expr, tablesampleKeyword string) string
	timeserieskeySQL                       func(g *Generator, expression *Expr) string
	tonumberSQL                            func(g *Generator, expression *Expr) string
	transactionSQL                         func(g *Generator, expression *Expr) string
	trimSQL                                func(g *Generator, expression *Expr) string
	trycastSQL                             func(g *Generator, expression *Expr) string
	tsordstotimeSQL                        func(g *Generator, expression *Expr) string
	uniquekeypropertySQL                   func(g *Generator, expression *Expr, prefix string) string
	unnestSQL                              func(g *Generator, expression *Expr) string
	usingpropertySQL                       func(g *Generator, expression *Expr) string
	uuidSQL                                func(g *Generator, expression *Expr) string
	valuesSQL                              func(g *Generator, expression *Expr, valuesAsTable bool) string
	versionSQL                             func(g *Generator, expression *Expr) string
	whileblockSQL                          func(g *Generator, expression *Expr) string
	windowSQL                              func(g *Generator, expression *Expr) string
	withProperties                         func(g *Generator, properties *Expr) string
	withingroupSQL                         func(g *Generator, expression *Expr) string
}

func defaultGeneratorHooks() generatorHooks {
	return generatorHooks{
		generate:                               (*Generator).baseGenerate,
		jsonpathkeySQL:                         (*Generator).baseJsonpathkeySQL,
		jsonpathsubscriptSQL:                   (*Generator).baseJsonpathsubscriptSQL,
		addColumnSQL:                           (*Generator).baseAddColumnSQL,
		afterLimitModifiers:                    (*Generator).baseAfterLimitModifiers,
		aliasesSQL:                             (*Generator).baseAliasesSQL,
		alterSQL:                               (*Generator).baseAlterSQL,
		altercolumnSQL:                         (*Generator).baseAltercolumnSQL,
		alterrenameSQL:                         (*Generator).baseAlterrenameSQL,
		altersetSQL:                            (*Generator).baseAltersetSQL,
		anyvalueSQL:                            (*Generator).baseAnyvalueSQL,
		arrayaggSQL:                            (*Generator).baseArrayaggSQL,
		attimezoneSQL:                          (*Generator).baseAttimezoneSQL,
		autoincrementcolumnconstraintSQL:       (*Generator).baseAutoincrementcolumnconstraintSQL,
		bitwisenotSQL:                          (*Generator).baseBitwisenotSQL,
		bitwisexorSQL:                          (*Generator).baseBitwisexorSQL,
		booleanSQL:                             (*Generator).baseBooleanSQL,
		bracketSQL:                             (*Generator).baseBracketSQL,
		castSQL:                                (*Generator).baseCastSQL,
		chrSQL:                                 (*Generator).baseChrSQL,
		clusterpropertySQL:                     (*Generator).baseClusterpropertySQL,
		collateSQL:                             (*Generator).baseCollateSQL,
		columnParts:                            (*Generator).baseColumnParts,
		columndefSQL:                           (*Generator).baseColumndefSQL,
		commitSQL:                              (*Generator).baseCommitSQL,
		computedcolumnconstraintSQL:            (*Generator).baseComputedcolumnconstraintSQL,
		concatwsSQL:                            (*Generator).baseConcatwsSQL,
		constraintSQL:                          (*Generator).baseConstraintSQL,
		convertSQL:                             (*Generator).baseConvertSQL,
		converttimezoneSQL:                     (*Generator).baseConverttimezoneSQL,
		createSQL:                              (*Generator).baseCreateSQL,
		createableSQL:                          (*Generator).baseCreateableSQL,
		cteSQL:                                 (*Generator).baseCteSQL,
		currentdateSQL:                         (*Generator).baseCurrentdateSQL,
		datatypeSQL:                            (*Generator).baseDatatypeSQL,
		deleteSQL:                              (*Generator).baseDeleteSQL,
		describeSQL:                            (*Generator).baseDescribeSQL,
		dotSQL:                                 (*Generator).baseDotSQL,
		dpipeSQL:                               (*Generator).baseDpipeSQL,
		dropSQL:                                (*Generator).baseDropSQL,
		dynamicidentifierSQL:                   (*Generator).baseDynamicidentifierSQL,
		eqSQL:                                  (*Generator).baseEqSQL,
		executeSQL:                             (*Generator).baseExecuteSQL,
		executesqlSQL:                          (*Generator).baseExecutesqlSQL,
		existsSQL:                              (*Generator).baseExistsSQL,
		extractSQL:                             (*Generator).baseExtractSQL,
		filterSQL:                              (*Generator).baseFilterSQL,
		formatTime:                             (*Generator).baseFormatTime,
		generatedasidentitycolumnconstraintSQL: (*Generator).baseGeneratedasidentitycolumnconstraintSQL,
		hexSQL:                                 (*Generator).baseHexSQL,
		hexstringSQL:                           (*Generator).baseHexstringSQL,
		hintSQL:                                (*Generator).baseHintSQL,
		identifierSQL:                          (*Generator).baseIdentifierSQL,
		ifblockSQL:                             (*Generator).baseIfblockSQL,
		ignorenullsSQL:                         (*Generator).baseIgnorenullsSQL,
		inSQL:                                  (*Generator).baseInSQL,
		inUnnestOp:                             (*Generator).baseInUnnestOp,
		indexcolumnconstraintSQL:               (*Generator).baseIndexcolumnconstraintSQL,
		installSQL:                             (*Generator).baseInstallSQL,
		intervalSQL:                            (*Generator).baseIntervalSQL,
		intoSQL:                                (*Generator).baseIntoSQL,
		isSQL:                                  (*Generator).baseIsSQL,
		joinSQL:                                (*Generator).baseJoinSQL,
		jsonpathSQL:                            (*Generator).baseJsonpathSQL,
		lambdaSQL:                              (*Generator).baseLambdaSQL,
		lateralOp:                              (*Generator).baseLateralOp,
		lateralSQL:                             (*Generator).baseLateralSQL,
		likepropertySQL:                        (*Generator).baseLikepropertySQL,
		locateProperties:                       (*Generator).baseLocateProperties,
		logSQL:                                 (*Generator).baseLogSQL,
		matchagainstSQL:                        (*Generator).baseMatchagainstSQL,
		modSQL:                                 (*Generator).baseModSQL,
		modelattributeSQL:                      (*Generator).baseModelattributeSQL,
		neqSQL:                                 (*Generator).baseNeqSQL,
		notSQL:                                 (*Generator).baseNotSQL,
		offsetLimitModifiers:                   (*Generator).baseOffsetLimitModifiers,
		offsetSQL:                              (*Generator).baseOffsetSQL,
		onclusterSQL:                           (*Generator).baseOnclusterSQL,
		optionsModifier:                        (*Generator).baseOptionsModifier,
		padSQL:                                 (*Generator).basePadSQL,
		parameterSQL:                           (*Generator).baseParameterSQL,
		parsedatetimeSQL:                       (*Generator).baseParsedatetimeSQL,
		parsejsonSQL:                           (*Generator).baseParsejsonSQL,
		partitionSQL:                           (*Generator).basePartitionSQL,
		partitionbyrangepropertySQL:            (*Generator).basePartitionbyrangepropertySQL,
		partitionbyrangepropertydynamicSQL:     (*Generator).basePartitionbyrangepropertydynamicSQL,
		partitionrangeSQL:                      (*Generator).basePartitionrangeSQL,
		placeholderSQL:                         (*Generator).basePlaceholderSQL,
		prewhereSQL:                            (*Generator).basePrewhereSQL,
		queryoptionSQL:                         (*Generator).baseQueryoptionSQL,
		randSQL:                                (*Generator).baseRandSQL,
		refreshtriggerpropertySQL:              (*Generator).baseRefreshtriggerpropertySQL,
		renamecolumnSQL:                        (*Generator).baseRenamecolumnSQL,
		respectnullsSQL:                        (*Generator).baseRespectnullsSQL,
		returningSQL:                           (*Generator).baseReturningSQL,
		rollbackSQL:                            (*Generator).baseRollbackSQL,
		schemaSQL:                              (*Generator).baseSchemaSQL,
		scopeResolution:                        (*Generator).baseScopeResolution,
		selectSQL:                              (*Generator).baseSelectSQL,
		setitemSQL:                             (*Generator).baseSetitemSQL,
		showSQL:                                (*Generator).baseShowSQL,
		spaceSQL:                               (*Generator).baseSpaceSQL,
		storedprocedureSQL:                     (*Generator).baseStoredprocedureSQL,
		strtodateSQL:                           (*Generator).baseStrtodateSQL,
		strtotimeSQL:                           (*Generator).baseStrtotimeSQL,
		structSQL:                              (*Generator).baseStructSQL,
		tableParts:                             (*Generator).baseTableParts,
		tableSQL:                               (*Generator).baseTableSQL,
		tablefromrowsSQL:                       (*Generator).baseTablefromrowsSQL,
		tablesampleSQL:                         (*Generator).baseTablesampleSQL,
		timeserieskeySQL:                       (*Generator).baseTimeserieskeySQL,
		tonumberSQL:                            (*Generator).baseTonumberSQL,
		transactionSQL:                         (*Generator).baseTransactionSQL,
		trimSQL:                                (*Generator).baseTrimSQL,
		trycastSQL:                             (*Generator).baseTrycastSQL,
		tsordstotimeSQL:                        (*Generator).baseTsordstotimeSQL,
		uniquekeypropertySQL:                   (*Generator).baseUniquekeypropertySQL,
		unnestSQL:                              (*Generator).baseUnnestSQL,
		usingpropertySQL:                       (*Generator).baseUsingpropertySQL,
		uuidSQL:                                (*Generator).baseUuidSQL,
		valuesSQL:                              (*Generator).baseValuesSQL,
		versionSQL:                             (*Generator).baseVersionSQL,
		whileblockSQL:                          (*Generator).baseWhileblockSQL,
		windowSQL:                              (*Generator).baseWindowSQL,
		withProperties:                         (*Generator).baseWithProperties,
		withingroupSQL:                         (*Generator).baseWithingroupSQL,
	}
}

func (g *Generator) jsonpathkeySQL(expression *Expr) string {
	return g.s.h.jsonpathkeySQL(g, expression)
}

func (g *Generator) jsonpathsubscriptSQL(expression *Expr) string {
	return g.s.h.jsonpathsubscriptSQL(g, expression)
}

func (g *Generator) addColumnSQL(expression *Expr) string {
	return g.s.h.addColumnSQL(g, expression)
}

func (g *Generator) afterLimitModifiers(expression *Expr) []string {
	return g.s.h.afterLimitModifiers(g, expression)
}

func (g *Generator) aliasesSQL(expression *Expr) string {
	return g.s.h.aliasesSQL(g, expression)
}

func (g *Generator) alterSQL(expression *Expr) string {
	return g.s.h.alterSQL(g, expression)
}

func (g *Generator) altercolumnSQL(expression *Expr) string {
	return g.s.h.altercolumnSQL(g, expression)
}

func (g *Generator) alterrenameSQL(expression *Expr, includeTo bool) string {
	return g.s.h.alterrenameSQL(g, expression, includeTo)
}

func (g *Generator) altersetSQL(expression *Expr) string {
	return g.s.h.altersetSQL(g, expression)
}

func (g *Generator) anyvalueSQL(expression *Expr) string {
	return g.s.h.anyvalueSQL(g, expression)
}

func (g *Generator) arrayaggSQL(expression *Expr) string {
	return g.s.h.arrayaggSQL(g, expression)
}

func (g *Generator) attimezoneSQL(expression *Expr) string {
	return g.s.h.attimezoneSQL(g, expression)
}

func (g *Generator) autoincrementcolumnconstraintSQL(expression *Expr) string {
	return g.s.h.autoincrementcolumnconstraintSQL(g, expression)
}

func (g *Generator) bitwisenotSQL(expression *Expr) string {
	return g.s.h.bitwisenotSQL(g, expression)
}

func (g *Generator) bitwisexorSQL(expression *Expr) string {
	return g.s.h.bitwisexorSQL(g, expression)
}

func (g *Generator) booleanSQL(expression *Expr) string {
	return g.s.h.booleanSQL(g, expression)
}

func (g *Generator) bracketSQL(expression *Expr) string {
	return g.s.h.bracketSQL(g, expression)
}

func (g *Generator) castSQL(expression *Expr, safePrefix string) string {
	return g.s.h.castSQL(g, expression, safePrefix)
}

func (g *Generator) chrSQL(expression *Expr, name string) string {
	return g.s.h.chrSQL(g, expression, name)
}

func (g *Generator) clusterpropertySQL(expression *Expr) string {
	return g.s.h.clusterpropertySQL(g, expression)
}

func (g *Generator) collateSQL(expression *Expr) string {
	return g.s.h.collateSQL(g, expression)
}

func (g *Generator) columnParts(expression *Expr) string {
	return g.s.h.columnParts(g, expression)
}

func (g *Generator) columndefSQL(expression *Expr, sep string) string {
	return g.s.h.columndefSQL(g, expression, sep)
}

func (g *Generator) commitSQL(expression *Expr) string {
	return g.s.h.commitSQL(g, expression)
}

func (g *Generator) computedcolumnconstraintSQL(expression *Expr) string {
	return g.s.h.computedcolumnconstraintSQL(g, expression)
}

func (g *Generator) concatwsSQL(expression *Expr) string {
	return g.s.h.concatwsSQL(g, expression)
}

func (g *Generator) constraintSQL(expression *Expr) string {
	return g.s.h.constraintSQL(g, expression)
}

func (g *Generator) convertSQL(expression *Expr) string {
	return g.s.h.convertSQL(g, expression)
}

func (g *Generator) converttimezoneSQL(expression *Expr) string {
	return g.s.h.converttimezoneSQL(g, expression)
}

func (g *Generator) createSQL(expression *Expr) string {
	return g.s.h.createSQL(g, expression)
}

func (g *Generator) createableSQL(expression *Expr, locations propLocations) string {
	return g.s.h.createableSQL(g, expression, locations)
}

func (g *Generator) cteSQL(expression *Expr) string {
	return g.s.h.cteSQL(g, expression)
}

func (g *Generator) currentdateSQL(expression *Expr) string {
	return g.s.h.currentdateSQL(g, expression)
}

func (g *Generator) datatypeSQL(expression *Expr) string {
	return g.s.h.datatypeSQL(g, expression)
}

func (g *Generator) deleteSQL(expression *Expr) string {
	return g.s.h.deleteSQL(g, expression)
}

func (g *Generator) describeSQL(expression *Expr) string {
	return g.s.h.describeSQL(g, expression)
}

func (g *Generator) dotSQL(expression *Expr) string {
	return g.s.h.dotSQL(g, expression)
}

func (g *Generator) dpipeSQL(expression *Expr) string {
	return g.s.h.dpipeSQL(g, expression)
}

func (g *Generator) dropSQL(expression *Expr) string {
	return g.s.h.dropSQL(g, expression)
}

func (g *Generator) dynamicidentifierSQL(expression *Expr) string {
	return g.s.h.dynamicidentifierSQL(g, expression)
}

func (g *Generator) eqSQL(expression *Expr) string {
	return g.s.h.eqSQL(g, expression)
}

func (g *Generator) executeSQL(expression *Expr) string {
	return g.s.h.executeSQL(g, expression)
}

func (g *Generator) executesqlSQL(expression *Expr) string {
	return g.s.h.executesqlSQL(g, expression)
}

func (g *Generator) existsSQL(expression *Expr) string {
	return g.s.h.existsSQL(g, expression)
}

func (g *Generator) extractSQL(expression *Expr) string {
	return g.s.h.extractSQL(g, expression)
}

func (g *Generator) filterSQL(expression *Expr) string {
	return g.s.h.filterSQL(g, expression)
}

func (g *Generator) formatTime(expression *Expr, inverseTimeMapping map[string]string, inverseTimeTrie *trie) string {
	return g.s.h.formatTime(g, expression, inverseTimeMapping, inverseTimeTrie)
}

func (g *Generator) generatedasidentitycolumnconstraintSQL(expression *Expr) string {
	return g.s.h.generatedasidentitycolumnconstraintSQL(g, expression)
}

func (g *Generator) hexSQL(expression *Expr) string {
	return g.s.h.hexSQL(g, expression)
}

func (g *Generator) hexstringSQL(expression *Expr, binaryFunctionRepr string) string {
	return g.s.h.hexstringSQL(g, expression, binaryFunctionRepr)
}

func (g *Generator) hintSQL(expression *Expr) string {
	return g.s.h.hintSQL(g, expression)
}

func (g *Generator) identifierSQL(expression *Expr) string {
	return g.s.h.identifierSQL(g, expression)
}

func (g *Generator) ifblockSQL(expression *Expr) string {
	return g.s.h.ifblockSQL(g, expression)
}

func (g *Generator) ignorenullsSQL(expression *Expr) string {
	return g.s.h.ignorenullsSQL(g, expression)
}

func (g *Generator) inSQL(expression *Expr) string {
	return g.s.h.inSQL(g, expression)
}

func (g *Generator) inUnnestOp(unnest *Expr) string {
	return g.s.h.inUnnestOp(g, unnest)
}

func (g *Generator) indexcolumnconstraintSQL(expression *Expr) string {
	return g.s.h.indexcolumnconstraintSQL(g, expression)
}

func (g *Generator) installSQL(expression *Expr) string {
	return g.s.h.installSQL(g, expression)
}

func (g *Generator) intervalSQL(expression *Expr) string {
	return g.s.h.intervalSQL(g, expression)
}

func (g *Generator) intoSQL(expression *Expr) string {
	return g.s.h.intoSQL(g, expression)
}

func (g *Generator) isSQL(expression *Expr) string {
	return g.s.h.isSQL(g, expression)
}

func (g *Generator) joinSQL(expression *Expr) string {
	return g.s.h.joinSQL(g, expression)
}

func (g *Generator) jsonpathSQL(expression *Expr) string {
	return g.s.h.jsonpathSQL(g, expression)
}

func (g *Generator) lambdaSQL(expression *Expr, arrowSep string, wrap bool) string {
	return g.s.h.lambdaSQL(g, expression, arrowSep, wrap)
}

func (g *Generator) lateralOp(expression *Expr) string {
	return g.s.h.lateralOp(g, expression)
}

func (g *Generator) lateralSQL(expression *Expr) string {
	return g.s.h.lateralSQL(g, expression)
}

func (g *Generator) likepropertySQL(expression *Expr) string {
	return g.s.h.likepropertySQL(g, expression)
}

func (g *Generator) locateProperties(properties *Expr) propLocations {
	return g.s.h.locateProperties(g, properties)
}

func (g *Generator) logSQL(expression *Expr) string {
	return g.s.h.logSQL(g, expression)
}

func (g *Generator) matchagainstSQL(expression *Expr) string {
	return g.s.h.matchagainstSQL(g, expression)
}

func (g *Generator) modSQL(expression *Expr) string {
	return g.s.h.modSQL(g, expression)
}

func (g *Generator) modelattributeSQL(expression *Expr) string {
	return g.s.h.modelattributeSQL(g, expression)
}

func (g *Generator) neqSQL(expression *Expr) string {
	return g.s.h.neqSQL(g, expression)
}

func (g *Generator) notSQL(expression *Expr) string {
	return g.s.h.notSQL(g, expression)
}

func (g *Generator) offsetLimitModifiers(expression *Expr, fetch bool, limit *Expr) []string {
	return g.s.h.offsetLimitModifiers(g, expression, fetch, limit)
}

func (g *Generator) offsetSQL(expression *Expr) string {
	return g.s.h.offsetSQL(g, expression)
}

func (g *Generator) onclusterSQL(expression *Expr) string {
	return g.s.h.onclusterSQL(g, expression)
}

func (g *Generator) optionsModifier(expression *Expr) string {
	return g.s.h.optionsModifier(g, expression)
}

func (g *Generator) padSQL(expression *Expr) string {
	return g.s.h.padSQL(g, expression)
}

func (g *Generator) parameterSQL(expression *Expr) string {
	return g.s.h.parameterSQL(g, expression)
}

func (g *Generator) parsedatetimeSQL(expression *Expr) string {
	return g.s.h.parsedatetimeSQL(g, expression)
}

func (g *Generator) parsejsonSQL(expression *Expr) string {
	return g.s.h.parsejsonSQL(g, expression)
}

func (g *Generator) partitionSQL(expression *Expr) string {
	return g.s.h.partitionSQL(g, expression)
}

func (g *Generator) partitionbyrangepropertySQL(expression *Expr) string {
	return g.s.h.partitionbyrangepropertySQL(g, expression)
}

func (g *Generator) partitionbyrangepropertydynamicSQL(expression *Expr) string {
	return g.s.h.partitionbyrangepropertydynamicSQL(g, expression)
}

func (g *Generator) partitionrangeSQL(expression *Expr) string {
	return g.s.h.partitionrangeSQL(g, expression)
}

func (g *Generator) placeholderSQL(expression *Expr) string {
	return g.s.h.placeholderSQL(g, expression)
}

func (g *Generator) prewhereSQL(expression *Expr) string {
	return g.s.h.prewhereSQL(g, expression)
}

func (g *Generator) queryoptionSQL(expression *Expr) string {
	return g.s.h.queryoptionSQL(g, expression)
}

func (g *Generator) randSQL(expression *Expr) string {
	return g.s.h.randSQL(g, expression)
}

func (g *Generator) refreshtriggerpropertySQL(expression *Expr) string {
	return g.s.h.refreshtriggerpropertySQL(g, expression)
}

func (g *Generator) renamecolumnSQL(expression *Expr) string {
	return g.s.h.renamecolumnSQL(g, expression)
}

func (g *Generator) respectnullsSQL(expression *Expr) string {
	return g.s.h.respectnullsSQL(g, expression)
}

func (g *Generator) returningSQL(expression *Expr) string {
	return g.s.h.returningSQL(g, expression)
}

func (g *Generator) rollbackSQL(expression *Expr) string {
	return g.s.h.rollbackSQL(g, expression)
}

func (g *Generator) schemaSQL(expression *Expr) string {
	return g.s.h.schemaSQL(g, expression)
}

func (g *Generator) scopeResolution(rhs string, scopeName string) string {
	return g.s.h.scopeResolution(g, rhs, scopeName)
}

func (g *Generator) selectSQL(expression *Expr) string {
	return g.s.h.selectSQL(g, expression)
}

func (g *Generator) setitemSQL(expression *Expr) string {
	return g.s.h.setitemSQL(g, expression)
}

func (g *Generator) showSQL(expression *Expr) string {
	return g.s.h.showSQL(g, expression)
}

func (g *Generator) spaceSQL(expression *Expr) string {
	return g.s.h.spaceSQL(g, expression)
}

func (g *Generator) storedprocedureSQL(expression *Expr) string {
	return g.s.h.storedprocedureSQL(g, expression)
}

func (g *Generator) strtodateSQL(expression *Expr) string {
	return g.s.h.strtodateSQL(g, expression)
}

func (g *Generator) strtotimeSQL(expression *Expr) string {
	return g.s.h.strtotimeSQL(g, expression)
}

func (g *Generator) structSQL(expression *Expr) string {
	return g.s.h.structSQL(g, expression)
}

func (g *Generator) tableParts(expression *Expr) string {
	return g.s.h.tableParts(g, expression)
}

func (g *Generator) tableSQL(expression *Expr, sep string) string {
	return g.s.h.tableSQL(g, expression, sep)
}

func (g *Generator) tablefromrowsSQL(expression *Expr) string {
	return g.s.h.tablefromrowsSQL(g, expression)
}

func (g *Generator) tablesampleSQL(expression *Expr, tablesampleKeyword string) string {
	return g.s.h.tablesampleSQL(g, expression, tablesampleKeyword)
}

func (g *Generator) timeserieskeySQL(expression *Expr) string {
	return g.s.h.timeserieskeySQL(g, expression)
}

func (g *Generator) tonumberSQL(expression *Expr) string {
	return g.s.h.tonumberSQL(g, expression)
}

func (g *Generator) transactionSQL(expression *Expr) string {
	return g.s.h.transactionSQL(g, expression)
}

func (g *Generator) trimSQL(expression *Expr) string {
	return g.s.h.trimSQL(g, expression)
}

func (g *Generator) trycastSQL(expression *Expr) string {
	return g.s.h.trycastSQL(g, expression)
}

func (g *Generator) tsordstotimeSQL(expression *Expr) string {
	return g.s.h.tsordstotimeSQL(g, expression)
}

func (g *Generator) uniquekeypropertySQL(expression *Expr, prefix string) string {
	return g.s.h.uniquekeypropertySQL(g, expression, prefix)
}

func (g *Generator) unnestSQL(expression *Expr) string {
	return g.s.h.unnestSQL(g, expression)
}

func (g *Generator) usingpropertySQL(expression *Expr) string {
	return g.s.h.usingpropertySQL(g, expression)
}

func (g *Generator) uuidSQL(expression *Expr) string {
	return g.s.h.uuidSQL(g, expression)
}

func (g *Generator) valuesSQL(expression *Expr, valuesAsTable bool) string {
	return g.s.h.valuesSQL(g, expression, valuesAsTable)
}

func (g *Generator) versionSQL(expression *Expr) string {
	return g.s.h.versionSQL(g, expression)
}

func (g *Generator) whileblockSQL(expression *Expr) string {
	return g.s.h.whileblockSQL(g, expression)
}

func (g *Generator) windowSQL(expression *Expr) string {
	return g.s.h.windowSQL(g, expression)
}

func (g *Generator) withProperties(properties *Expr) string {
	return g.s.h.withProperties(g, properties)
}

func (g *Generator) withingroupSQL(expression *Expr) string {
	return g.s.h.withingroupSQL(g, expression)
}
