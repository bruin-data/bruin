package sqlengine

import "iter"

var _ iter.Seq[*Expr]

// parserHooks holds the parser methods that dialects override (virtual dispatch).
// Base implementations are named base<Method>; call sites use the wrapper methods.
type parserHooks struct {
	negateRange                        func(p *Parser, this *Expr) *Expr
	parseAlias                         func(p *Parser, this *Expr, explicit bool) *Expr
	parseAlterDropAction               func(p *Parser) *Expr
	parseAlterTableAlter               func(p *Parser) *Expr
	parseAlterTableRename              func(p *Parser) *Expr
	parseAlterTableSet                 func(p *Parser) *Expr
	parseAssignment                    func(p *Parser) *Expr
	parseBracket                       func(p *Parser, this *Expr) *Expr
	parseBracketKeyValue               func(p *Parser, isMap bool) *Expr
	parseCharsetName                   func(p *Parser) *Expr
	parseCheckConstraint               func(p *Parser) *Expr
	parseClusterProperty               func(p *Parser) *Expr
	parseColumn                        func(p *Parser) *Expr
	parseColumnDef                     func(p *Parser, this *Expr, computedColumn bool) *Expr
	parseColumnOps                     func(p *Parser, this *Expr) *Expr
	parseCommitOrRollback              func(p *Parser) *Expr
	parseConnectWithPrior              func(p *Parser) *Expr
	parseConstraint                    func(p *Parser) *Expr
	parseConvert                       func(p *Parser, strict bool, safe bool) *Expr
	parseCreate                        func(p *Parser) *Expr
	parseCte                           func(p *Parser) *Expr
	parseDcolon                        func(p *Parser) *Expr
	parseDefiner                       func(p *Parser) *Expr
	parseDescribe                      func(p *Parser) *Expr
	parseDropColumn                    func(p *Parser) *Expr
	parseExpression                    func(p *Parser) *Expr
	parseExtract                       func(p *Parser) *Expr
	parseFileLocation                  func(p *Parser) *Expr
	parseForeignKey                    func(p *Parser) *Expr
	parseFunction                      func(p *Parser, functions map[string]FuncBuilder, anonymous bool, optionalParens bool, anyToken bool) *Expr
	parseFunctionCall                  func(p *Parser, functions map[string]FuncBuilder, anonymous bool, optionalParens bool, anyToken bool) *Expr
	parseFunctionParameter             func(p *Parser) *Expr
	parseFunctionProperties            func(p *Parser) *Expr
	parseGeneratedAsIdentity           func(p *Parser) *Expr
	parseGroupConcat                   func(p *Parser) *Expr
	parseHintFunctionCall              func(p *Parser) *Expr
	parseIdVar                         func(p *Parser, anyToken bool, tokens *TokenSet) *Expr
	parseIf                            func(p *Parser) *Expr
	parseInsertTable                   func(p *Parser) *Expr
	parseInto                          func(p *Parser) *Expr
	parseJoin                          func(p *Parser, skipJoinToken bool, parseBracket bool, aliasTokens *TokenSet) *Expr
	parseJoinParts                     func(p *Parser) (*Token, *Token, *Token)
	parseJsonObject                    func(p *Parser, agg bool) *Expr
	parseLambda                        func(p *Parser, alias bool) *Expr
	parseLambdaArg                     func(p *Parser) *Expr
	parseLateral                       func(p *Parser) *Expr
	parseOnProperty                    func(p *Parser) *Expr
	parseParameter                     func(p *Parser) *Expr
	parsePartition                     func(p *Parser) *Expr
	parsePartitionAndOrder             func(p *Parser) ([]*Expr, *Expr)
	parsePartitionedBy                 func(p *Parser) *Expr
	parsePivotAggregation              func(p *Parser) *Expr
	parsePosition                      func(p *Parser, haystackFirst bool) *Expr
	parsePrimary                       func(p *Parser) *Expr
	parsePrimaryKey                    func(p *Parser, wrappedOptional bool, inProps bool, namedPrimaryKey bool) *Expr
	parsePrimaryKeyPart                func(p *Parser) *Expr
	parseProjections                   func(p *Parser) ([]*Expr, []*Expr)
	parsePropertyBefore                func(p *Parser) any
	parseReturns                       func(p *Parser) *Expr
	parseSet                           func(p *Parser, unset bool, tag bool) *Expr
	parseStructTypes                   func(p *Parser, typeRequired bool) *Expr
	parseSubstring                     func(p *Parser) *Expr
	parseTable                         func(p *Parser, schema bool, joins bool, aliasTokens *TokenSet, parseBracket bool, isDbReference bool, parsePartition bool, consumePipe bool) *Expr
	parseTablePart                     func(p *Parser, schema bool) *Expr
	parseTableParts                    func(p *Parser, schema bool, isDbReference bool, wildcard bool, fast bool) *Expr
	parseTableSample                   func(p *Parser, asModifier bool) *Expr
	parseTransaction                   func(p *Parser) *Expr
	parseType                          func(p *Parser, parseInterval bool, fallbackToIdentifier bool) *Expr
	parseTypes                         func(p *Parser, checkFunc bool, schema bool, allowIdentifiers bool, withCollation bool) *Expr
	parseUnique                        func(p *Parser) *Expr
	parseUniqueKey                     func(p *Parser) *Expr
	parseUnnest                        func(p *Parser, withAlias bool) *Expr
	parseUpdate                        func(p *Parser) *Expr
	parseUse                           func(p *Parser) *Expr
	parseUserDefinedFunction           func(p *Parser, kind TokenType) *Expr
	parseUserDefinedFunctionExpression func(p *Parser) *Expr
	parseUserDefinedType               func(p *Parser, identifier *Expr) *Expr
	parseValue                         func(p *Parser, values bool) *Expr
	parseWindow                        func(p *Parser, this *Expr, alias bool) *Expr
	parseWithProperty                  func(p *Parser) any
	parseWrappedIdVars                 func(p *Parser, optional bool) []*Expr
	parseWrappedSelect                 func(p *Parser, table bool) *Expr
	pivotColumnNames                   func(p *Parser, aggregations []*Expr) []string
	toPropEq                           func(p *Parser, expression *Expr, index int) *Expr
	buildCast                          func(p *Parser, strict bool, kv ...any) *Expr
}

func defaultParserHooks() parserHooks {
	return parserHooks{
		negateRange:                        (*Parser).baseNegateRange,
		parseAlias:                         (*Parser).baseParseAlias,
		parseAlterDropAction:               (*Parser).baseParseAlterDropAction,
		parseAlterTableAlter:               (*Parser).baseParseAlterTableAlter,
		parseAlterTableRename:              (*Parser).baseParseAlterTableRename,
		parseAlterTableSet:                 (*Parser).baseParseAlterTableSet,
		parseAssignment:                    (*Parser).baseParseAssignment,
		parseBracket:                       (*Parser).baseParseBracket,
		parseBracketKeyValue:               (*Parser).baseParseBracketKeyValue,
		parseCharsetName:                   (*Parser).baseParseCharsetName,
		parseCheckConstraint:               (*Parser).baseParseCheckConstraint,
		parseClusterProperty:               (*Parser).baseParseClusterProperty,
		parseColumn:                        (*Parser).baseParseColumn,
		parseColumnDef:                     (*Parser).baseParseColumnDef,
		parseColumnOps:                     (*Parser).baseParseColumnOps,
		parseCommitOrRollback:              (*Parser).baseParseCommitOrRollback,
		parseConnectWithPrior:              (*Parser).baseParseConnectWithPrior,
		parseConstraint:                    (*Parser).baseParseConstraint,
		parseConvert:                       (*Parser).baseParseConvert,
		parseCreate:                        (*Parser).baseParseCreate,
		parseCte:                           (*Parser).baseParseCte,
		parseDcolon:                        (*Parser).baseParseDcolon,
		parseDefiner:                       (*Parser).baseParseDefiner,
		parseDescribe:                      (*Parser).baseParseDescribe,
		parseDropColumn:                    (*Parser).baseParseDropColumn,
		parseExpression:                    (*Parser).baseParseExpression,
		parseExtract:                       (*Parser).baseParseExtract,
		parseFileLocation:                  (*Parser).baseParseFileLocation,
		parseForeignKey:                    (*Parser).baseParseForeignKey,
		parseFunction:                      (*Parser).baseParseFunction,
		parseFunctionCall:                  (*Parser).baseParseFunctionCall,
		parseFunctionParameter:             (*Parser).baseParseFunctionParameter,
		parseFunctionProperties:            (*Parser).baseParseFunctionProperties,
		parseGeneratedAsIdentity:           (*Parser).baseParseGeneratedAsIdentity,
		parseGroupConcat:                   (*Parser).baseParseGroupConcat,
		parseHintFunctionCall:              (*Parser).baseParseHintFunctionCall,
		parseIdVar:                         (*Parser).baseParseIdVar,
		parseIf:                            (*Parser).baseParseIf,
		parseInsertTable:                   (*Parser).baseParseInsertTable,
		parseInto:                          (*Parser).baseParseInto,
		parseJoin:                          (*Parser).baseParseJoin,
		parseJoinParts:                     (*Parser).baseParseJoinParts,
		parseJsonObject:                    (*Parser).baseParseJsonObject,
		parseLambda:                        (*Parser).baseParseLambda,
		parseLambdaArg:                     (*Parser).baseParseLambdaArg,
		parseLateral:                       (*Parser).baseParseLateral,
		parseOnProperty:                    (*Parser).baseParseOnProperty,
		parseParameter:                     (*Parser).baseParseParameter,
		parsePartition:                     (*Parser).baseParsePartition,
		parsePartitionAndOrder:             (*Parser).baseParsePartitionAndOrder,
		parsePartitionedBy:                 (*Parser).baseParsePartitionedBy,
		parsePivotAggregation:              (*Parser).baseParsePivotAggregation,
		parsePosition:                      (*Parser).baseParsePosition,
		parsePrimary:                       (*Parser).baseParsePrimary,
		parsePrimaryKey:                    (*Parser).baseParsePrimaryKey,
		parsePrimaryKeyPart:                (*Parser).baseParsePrimaryKeyPart,
		parseProjections:                   (*Parser).baseParseProjections,
		parsePropertyBefore:                (*Parser).baseParsePropertyBefore,
		parseReturns:                       (*Parser).baseParseReturns,
		parseSet:                           (*Parser).baseParseSet,
		parseStructTypes:                   (*Parser).baseParseStructTypes,
		parseSubstring:                     (*Parser).baseParseSubstring,
		parseTable:                         (*Parser).baseParseTable,
		parseTablePart:                     (*Parser).baseParseTablePart,
		parseTableParts:                    (*Parser).baseParseTableParts,
		parseTableSample:                   (*Parser).baseParseTableSample,
		parseTransaction:                   (*Parser).baseParseTransaction,
		parseType:                          (*Parser).baseParseType,
		parseTypes:                         (*Parser).baseParseTypes,
		parseUnique:                        (*Parser).baseParseUnique,
		parseUniqueKey:                     (*Parser).baseParseUniqueKey,
		parseUnnest:                        (*Parser).baseParseUnnest,
		parseUpdate:                        (*Parser).baseParseUpdate,
		parseUse:                           (*Parser).baseParseUse,
		parseUserDefinedFunction:           (*Parser).baseParseUserDefinedFunction,
		parseUserDefinedFunctionExpression: (*Parser).baseParseUserDefinedFunctionExpression,
		parseUserDefinedType:               (*Parser).baseParseUserDefinedType,
		parseValue:                         (*Parser).baseParseValue,
		parseWindow:                        (*Parser).baseParseWindow,
		parseWithProperty:                  (*Parser).baseParseWithProperty,
		parseWrappedIdVars:                 (*Parser).baseParseWrappedIdVars,
		parseWrappedSelect:                 (*Parser).baseParseWrappedSelect,
		pivotColumnNames:                   (*Parser).basePivotColumnNames,
		toPropEq:                           (*Parser).baseToPropEq,
		buildCast:                          (*Parser).baseBuildCast,
	}
}

func (p *Parser) negateRange(this *Expr) *Expr {
	return p.s.h.negateRange(p, this)
}

func (p *Parser) parseAlias(this *Expr, explicit bool) *Expr {
	return p.s.h.parseAlias(p, this, explicit)
}

func (p *Parser) parseAlterDropAction() *Expr {
	return p.s.h.parseAlterDropAction(p)
}

func (p *Parser) parseAlterTableAlter() *Expr {
	return p.s.h.parseAlterTableAlter(p)
}

func (p *Parser) parseAlterTableRename() *Expr {
	return p.s.h.parseAlterTableRename(p)
}

func (p *Parser) parseAlterTableSet() *Expr {
	return p.s.h.parseAlterTableSet(p)
}

func (p *Parser) parseAssignment() *Expr {
	p.enter()
	defer p.leave()
	return p.s.h.parseAssignment(p)
}

func (p *Parser) parseBracket(this *Expr) *Expr {
	return p.s.h.parseBracket(p, this)
}

func (p *Parser) parseBracketKeyValue(isMap bool) *Expr {
	return p.s.h.parseBracketKeyValue(p, isMap)
}

func (p *Parser) parseCharsetName() *Expr {
	return p.s.h.parseCharsetName(p)
}

func (p *Parser) parseCheckConstraint() *Expr {
	return p.s.h.parseCheckConstraint(p)
}

func (p *Parser) parseClusterProperty() *Expr {
	return p.s.h.parseClusterProperty(p)
}

func (p *Parser) parseColumn() *Expr {
	return p.s.h.parseColumn(p)
}

func (p *Parser) parseColumnDef(this *Expr, computedColumn bool) *Expr {
	return p.s.h.parseColumnDef(p, this, computedColumn)
}

func (p *Parser) parseColumnOps(this *Expr) *Expr {
	return p.s.h.parseColumnOps(p, this)
}

func (p *Parser) parseCommitOrRollback() *Expr {
	return p.s.h.parseCommitOrRollback(p)
}

func (p *Parser) parseConnectWithPrior() *Expr {
	return p.s.h.parseConnectWithPrior(p)
}

func (p *Parser) parseConstraint() *Expr {
	return p.s.h.parseConstraint(p)
}

func (p *Parser) parseConvert(strict bool, safe bool) *Expr {
	return p.s.h.parseConvert(p, strict, safe)
}

func (p *Parser) parseCreate() *Expr {
	return p.s.h.parseCreate(p)
}

func (p *Parser) parseCte() *Expr {
	return p.s.h.parseCte(p)
}

func (p *Parser) parseDcolon() *Expr {
	return p.s.h.parseDcolon(p)
}

func (p *Parser) parseDefiner() *Expr {
	return p.s.h.parseDefiner(p)
}

func (p *Parser) parseDescribe() *Expr {
	return p.s.h.parseDescribe(p)
}

func (p *Parser) parseDropColumn() *Expr {
	return p.s.h.parseDropColumn(p)
}

func (p *Parser) parseExpression() *Expr {
	p.enter()
	defer p.leave()
	return p.s.h.parseExpression(p)
}

func (p *Parser) parseExtract() *Expr {
	return p.s.h.parseExtract(p)
}

func (p *Parser) parseFileLocation() *Expr {
	return p.s.h.parseFileLocation(p)
}

func (p *Parser) parseForeignKey() *Expr {
	return p.s.h.parseForeignKey(p)
}

func (p *Parser) parseFunction(functions map[string]FuncBuilder, anonymous bool, optionalParens bool, anyToken bool) *Expr {
	return p.s.h.parseFunction(p, functions, anonymous, optionalParens, anyToken)
}

func (p *Parser) parseFunctionCall(functions map[string]FuncBuilder, anonymous bool, optionalParens bool, anyToken bool) *Expr {
	return p.s.h.parseFunctionCall(p, functions, anonymous, optionalParens, anyToken)
}

func (p *Parser) parseFunctionParameter() *Expr {
	return p.s.h.parseFunctionParameter(p)
}

func (p *Parser) parseFunctionProperties() *Expr {
	return p.s.h.parseFunctionProperties(p)
}

func (p *Parser) parseGeneratedAsIdentity() *Expr {
	return p.s.h.parseGeneratedAsIdentity(p)
}

func (p *Parser) parseGroupConcat() *Expr {
	return p.s.h.parseGroupConcat(p)
}

func (p *Parser) parseHintFunctionCall() *Expr {
	return p.s.h.parseHintFunctionCall(p)
}

func (p *Parser) parseIdVar(anyToken bool, tokens *TokenSet) *Expr {
	return p.s.h.parseIdVar(p, anyToken, tokens)
}

func (p *Parser) parseIf() *Expr {
	return p.s.h.parseIf(p)
}

func (p *Parser) parseInsertTable() *Expr {
	return p.s.h.parseInsertTable(p)
}

func (p *Parser) parseInto() *Expr {
	return p.s.h.parseInto(p)
}

func (p *Parser) parseJoin(skipJoinToken bool, parseBracket bool, aliasTokens *TokenSet) *Expr {
	return p.s.h.parseJoin(p, skipJoinToken, parseBracket, aliasTokens)
}

func (p *Parser) parseJoinParts() (*Token, *Token, *Token) {
	return p.s.h.parseJoinParts(p)
}

func (p *Parser) parseJsonObject(agg bool) *Expr {
	return p.s.h.parseJsonObject(p, agg)
}

func (p *Parser) parseLambda(alias bool) *Expr {
	return p.s.h.parseLambda(p, alias)
}

func (p *Parser) parseLambdaArg() *Expr {
	return p.s.h.parseLambdaArg(p)
}

func (p *Parser) parseLateral() *Expr {
	return p.s.h.parseLateral(p)
}

func (p *Parser) parseOnProperty() *Expr {
	return p.s.h.parseOnProperty(p)
}

func (p *Parser) parseParameter() *Expr {
	return p.s.h.parseParameter(p)
}

func (p *Parser) parsePartition() *Expr {
	return p.s.h.parsePartition(p)
}

func (p *Parser) parsePartitionAndOrder() ([]*Expr, *Expr) {
	return p.s.h.parsePartitionAndOrder(p)
}

func (p *Parser) parsePartitionedBy() *Expr {
	return p.s.h.parsePartitionedBy(p)
}

func (p *Parser) parsePivotAggregation() *Expr {
	return p.s.h.parsePivotAggregation(p)
}

func (p *Parser) parsePosition(haystackFirst bool) *Expr {
	return p.s.h.parsePosition(p, haystackFirst)
}

func (p *Parser) parsePrimary() *Expr {
	p.enter()
	defer p.leave()
	return p.s.h.parsePrimary(p)
}

func (p *Parser) parsePrimaryKey(wrappedOptional bool, inProps bool, namedPrimaryKey bool) *Expr {
	return p.s.h.parsePrimaryKey(p, wrappedOptional, inProps, namedPrimaryKey)
}

func (p *Parser) parsePrimaryKeyPart() *Expr {
	return p.s.h.parsePrimaryKeyPart(p)
}

func (p *Parser) parseProjections() ([]*Expr, []*Expr) {
	return p.s.h.parseProjections(p)
}

func (p *Parser) parsePropertyBefore() any {
	return p.s.h.parsePropertyBefore(p)
}

func (p *Parser) parseReturns() *Expr {
	return p.s.h.parseReturns(p)
}

func (p *Parser) parseSet(unset bool, tag bool) *Expr {
	return p.s.h.parseSet(p, unset, tag)
}

func (p *Parser) parseStructTypes(typeRequired bool) *Expr {
	return p.s.h.parseStructTypes(p, typeRequired)
}

func (p *Parser) parseSubstring() *Expr {
	return p.s.h.parseSubstring(p)
}

func (p *Parser) parseTable(schema bool, joins bool, aliasTokens *TokenSet, parseBracket bool, isDbReference bool, parsePartition bool, consumePipe bool) *Expr {
	p.enter()
	defer p.leave()
	return p.s.h.parseTable(p, schema, joins, aliasTokens, parseBracket, isDbReference, parsePartition, consumePipe)
}

func (p *Parser) parseTablePart(schema bool) *Expr {
	return p.s.h.parseTablePart(p, schema)
}

func (p *Parser) parseTableParts(schema bool, isDbReference bool, wildcard bool, fast bool) *Expr {
	return p.s.h.parseTableParts(p, schema, isDbReference, wildcard, fast)
}

func (p *Parser) parseTableSample(asModifier bool) *Expr {
	return p.s.h.parseTableSample(p, asModifier)
}

func (p *Parser) parseTransaction() *Expr {
	return p.s.h.parseTransaction(p)
}

func (p *Parser) parseType(parseInterval bool, fallbackToIdentifier bool) *Expr {
	return p.s.h.parseType(p, parseInterval, fallbackToIdentifier)
}

func (p *Parser) parseTypes(checkFunc bool, schema bool, allowIdentifiers bool, withCollation bool) *Expr {
	p.enter()
	defer p.leave()
	return p.s.h.parseTypes(p, checkFunc, schema, allowIdentifiers, withCollation)
}

func (p *Parser) parseUnique() *Expr {
	return p.s.h.parseUnique(p)
}

func (p *Parser) parseUniqueKey() *Expr {
	return p.s.h.parseUniqueKey(p)
}

func (p *Parser) parseUnnest(withAlias bool) *Expr {
	return p.s.h.parseUnnest(p, withAlias)
}

func (p *Parser) parseUpdate() *Expr {
	return p.s.h.parseUpdate(p)
}

func (p *Parser) parseUse() *Expr {
	return p.s.h.parseUse(p)
}

func (p *Parser) parseUserDefinedFunction(kind TokenType) *Expr {
	return p.s.h.parseUserDefinedFunction(p, kind)
}

func (p *Parser) parseUserDefinedFunctionExpression() *Expr {
	return p.s.h.parseUserDefinedFunctionExpression(p)
}

func (p *Parser) parseUserDefinedType(identifier *Expr) *Expr {
	return p.s.h.parseUserDefinedType(p, identifier)
}

func (p *Parser) parseValue(values bool) *Expr {
	return p.s.h.parseValue(p, values)
}

func (p *Parser) parseWindow(this *Expr, alias bool) *Expr {
	return p.s.h.parseWindow(p, this, alias)
}

func (p *Parser) parseWithProperty() any {
	return p.s.h.parseWithProperty(p)
}

func (p *Parser) parseWrappedIdVars(optional bool) []*Expr {
	return p.s.h.parseWrappedIdVars(p, optional)
}

func (p *Parser) parseWrappedSelect(table bool) *Expr {
	return p.s.h.parseWrappedSelect(p, table)
}

func (p *Parser) pivotColumnNames(aggregations []*Expr) []string {
	return p.s.h.pivotColumnNames(p, aggregations)
}

func (p *Parser) toPropEq(expression *Expr, index int) *Expr {
	return p.s.h.toPropEq(p, expression, index)
}

func (p *Parser) buildCast(strict bool, kv ...any) *Expr {
	return p.s.h.buildCast(p, strict, kv...)
}
