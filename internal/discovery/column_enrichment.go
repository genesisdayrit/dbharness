package discovery

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

const (
	columnProfileSampleValueLimit = 5
	maxColumnSampleValueLength    = 180
)

func newEnrichedColumnInfo(column ColumnInfo) EnrichedColumnInfo {
	return EnrichedColumnInfo{
		Name:            column.Name,
		DataType:        column.DataType,
		IsNullable:      column.IsNullable,
		OrdinalPosition: column.OrdinalPosition,
		ColumnDefault:   column.ColumnDefault,
		AIDescription:   "",
		DBDescription:   "",
	}
}

func percentOfTotal(numerator, denominator int64) float64 {
	if denominator <= 0 {
		return 0
	}
	value := (float64(numerator) / float64(denominator)) * 100
	return math.Round(value*10000) / 10000
}

func shouldSkipColumnSamples(dataType string) bool {
	lower := strings.ToLower(strings.TrimSpace(dataType))
	return strings.Contains(lower, "vector")
}

func normalizeColumnSampleValues(values []string) []string {
	if len(values) == 0 {
		return nil
	}

	seen := make(map[string]bool, len(values))
	out := make([]string, 0, columnProfileSampleValueLimit)
	for _, raw := range values {
		value := strings.TrimSpace(raw)
		if value == "" {
			continue
		}
		value = truncateColumnSampleValue(value)
		if seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
		if len(out) >= columnProfileSampleValueLimit {
			break
		}
	}

	return out
}

func truncateColumnSampleValue(value string) string {
	if len(value) <= maxColumnSampleValueLength {
		return value
	}
	return value[:maxColumnSampleValueLength-3] + "..."
}

func int64FromDBValue(value interface{}) (int64, error) {
	switch v := value.(type) {
	case nil:
		return 0, nil
	case int64:
		return v, nil
	case int32:
		return int64(v), nil
	case int:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case float32:
		return int64(v), nil
	case []byte:
		return parseInt64String(string(v))
	case string:
		return parseInt64String(v)
	default:
		return parseInt64String(formatValue(v))
	}
}

func parseInt64String(raw string) (int64, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return 0, nil
	}
	if iv, err := strconv.ParseInt(value, 10, 64); err == nil {
		return iv, nil
	}
	fv, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, fmt.Errorf("parse int64 from %q: %w", value, err)
	}
	return int64(fv), nil
}

func quotePostgresIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func quoteSnowflakeIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func quoteMySQLIdentifier(identifier string) string {
	return "`" + strings.ReplaceAll(identifier, "`", "``") + "`"
}

func quoteBigQueryIdentifier(identifier string) string {
	return "`" + strings.ReplaceAll(identifier, "`", "``") + "`"
}
