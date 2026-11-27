package postgres

import (
	"encoding/binary"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// valueConverter rewrites one decoded column value. raw is the column's wire bytes.
type valueConverter func(raw []byte, value any) any

// resultValueConverters returns a converter per column, or nil for columns pgx already
// decodes into values that need no column context. It covers values whose Go form can't
// be read correctly without the column type: pgx returns xml as []byte like bytea and
// "char" as an int32 like int4, and flattens multi-dimensional arrays. bytea stays []byte,
// since callers such as inference need the raw bytes. Returns nil if no column needs one.
func resultValueConverters(fields []pgconn.FieldDescription, typeMap *pgtype.Map) []valueConverter {
	var converters []valueConverter
	for i, field := range fields {
		convert := scalarConverter(field.DataTypeOID)
		if convert == nil {
			convert = arrayConverter(field, typeMap)
		}
		if convert == nil {
			continue
		}
		if converters == nil {
			converters = make([]valueConverter, len(fields))
		}
		converters[i] = convert
	}
	return converters
}

func convertResultValues(converters []valueConverter, rawValues [][]byte, values []any) {
	for i, convert := range converters {
		if convert == nil || values[i] == nil {
			continue
		}
		var raw []byte
		if i < len(rawValues) {
			raw = rawValues[i]
		}
		values[i] = convert(raw, values[i])
	}
}

func scalarConverter(oid uint32) valueConverter {
	switch oid {
	case pgtype.XMLOID:
		return func(_ []byte, value any) any {
			if b, ok := value.([]byte); ok {
				return string(b)
			}
			return value
		}
	case pgtype.QCharOID:
		return func(_ []byte, value any) any {
			if c, ok := value.(int32); ok {
				return formatQChar(c)
			}
			return value
		}
	}
	return nil
}

// formatQChar matches Postgres' output for the single-byte "char" type, which pgx
// decodes as an int32 holding the byte.
func formatQChar(c int32) string {
	switch {
	case c == 0:
		return ""
	case c >= 0x80:
		return fmt.Sprintf(`\%03o`, c)
	default:
		return fmt.Sprintf("%c", c)
	}
}

func arrayConverter(field pgconn.FieldDescription, typeMap *pgtype.Map) valueConverter {
	dataType, ok := typeMap.TypeForOID(field.DataTypeOID)
	if !ok {
		return nil
	}
	arrayCodec, ok := dataType.Codec.(*pgtype.ArrayCodec)
	if !ok {
		return nil
	}
	convertElement := scalarConverter(arrayCodec.ElementType.OID)
	binaryFormat := field.Format == pgtype.BinaryFormatCode

	return func(raw []byte, value any) any {
		elements, ok := value.([]any)
		if !ok {
			return value
		}
		if convertElement != nil {
			for i, element := range elements {
				if element != nil {
					elements[i] = convertElement(nil, element)
				}
			}
		}
		if binaryFormat {
			return nestArrayElements(raw, elements)
		}
		return elements
	}
}

// nestArrayElements reshapes the flat slice pgx decodes a multi-dimensional array into,
// so {{1,2},{3,4}} becomes [[1 2] [3 4]] rather than [1 2 3 4]. The dimensions come from
// the binary array header: ndims, has-null flag and element OID, then length and lower
// bound per dimension, each a 4-byte integer.
func nestArrayElements(raw []byte, elements []any) []any {
	// Postgres arrays have at most 6 dimensions (MAXDIM).
	const headerSize, dimensionSize, maxDimensions = 12, 8, 6
	if len(raw) < headerSize {
		return elements
	}
	ndims := binary.BigEndian.Uint32(raw)
	if ndims < 2 || ndims > maxDimensions || len(raw) < headerSize+dimensionSize*int(ndims) {
		return elements
	}

	lengths := make([]int, ndims)
	total := 1
	for i := range lengths {
		length := binary.BigEndian.Uint32(raw[headerSize+dimensionSize*i:])
		if length == 0 || int(length) > len(elements) {
			return elements
		}
		lengths[i] = int(length)
		total *= lengths[i]
		if total > len(elements) {
			return elements
		}
	}
	if total != len(elements) {
		return elements
	}
	return reshapeArray(elements, lengths)
}

func reshapeArray(elements []any, lengths []int) []any {
	if len(lengths) == 1 {
		return elements
	}
	step := len(elements) / lengths[0]
	nested := make([]any, lengths[0])
	for i := range nested {
		nested[i] = reshapeArray(elements[i*step:(i+1)*step], lengths[1:])
	}
	return nested
}
