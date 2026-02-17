package discovery

import (
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
)

func TestFormatValue(t *testing.T) {
	tests := []struct {
		name  string
		input interface{}
		want  string
	}{
		{name: "nil", input: nil, want: ""},
		{name: "string", input: "hello", want: "hello"},
		{name: "bytes", input: []byte("binary data"), want: "binary data"},
		{name: "bool true", input: true, want: "true"},
		{name: "bool false", input: false, want: "false"},
		{name: "int64", input: int64(42), want: "42"},
		{name: "int64 negative", input: int64(-100), want: "-100"},
		{name: "int32", input: int32(7), want: "7"},
		{name: "int", input: 99, want: "99"},
		{name: "float64 integer-like", input: float64(3), want: "3"},
		{name: "float64 decimal", input: float64(3.14), want: "3.14"},
		{name: "float32", input: float32(2.5), want: "2.5"},
		{name: "time date only", input: time.Date(2026, 2, 16, 0, 0, 0, 0, time.UTC), want: "2026-02-16"},
		{name: "time with time", input: time.Date(2026, 2, 16, 14, 30, 0, 0, time.UTC), want: "2026-02-16T14:30:00Z"},
		{name: "json raw", input: json.RawMessage(`{"key":"value"}`), want: `{"key":"value"}`},
		{name: "null string valid", input: sql.NullString{String: "ok", Valid: true}, want: "ok"},
		{name: "null string invalid", input: sql.NullString{Valid: false}, want: ""},
		{name: "null int64 valid", input: sql.NullInt64{Int64: 7, Valid: true}, want: "7"},
		{name: "null int64 invalid", input: sql.NullInt64{Valid: false}, want: ""},
		{name: "null float64 valid", input: sql.NullFloat64{Float64: 1.5, Valid: true}, want: "1.5"},
		{name: "null float64 invalid", input: sql.NullFloat64{Valid: false}, want: ""},
		{name: "null bool valid", input: sql.NullBool{Bool: true, Valid: true}, want: "true"},
		{name: "null bool invalid", input: sql.NullBool{Valid: false}, want: ""},
		{name: "null time valid", input: sql.NullTime{Time: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true}, want: "2026-01-01"},
		{name: "null time invalid", input: sql.NullTime{Valid: false}, want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatValue(tt.input)
			if got != tt.want {
				t.Fatalf("formatValue(%v) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestPercentOfTotal(t *testing.T) {
	tests := []struct {
		name        string
		numerator   int64
		denominator int64
		want        float64
	}{
		{name: "normal fraction", numerator: 1, denominator: 3, want: 33.3333},
		{name: "full", numerator: 5, denominator: 5, want: 100},
		{name: "empty denominator", numerator: 5, denominator: 0, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := percentOfTotal(tt.numerator, tt.denominator)
			if got != tt.want {
				t.Fatalf("percentOfTotal(%d, %d) = %v, want %v", tt.numerator, tt.denominator, got, tt.want)
			}
		})
	}
}

func TestNormalizeColumnSampleValues(t *testing.T) {
	veryLong := strings.Repeat("x", maxColumnSampleValueLength+25)

	got := normalizeColumnSampleValues([]string{
		"  alpha  ",
		"alpha",
		"",
		veryLong,
		"beta",
		"gamma",
		"delta",
		"epsilon",
		"zeta",
	})

	if len(got) != columnProfileSampleValueLimit {
		t.Fatalf("sample value count = %d, want %d", len(got), columnProfileSampleValueLimit)
	}
	if got[0] != "alpha" {
		t.Fatalf("first value = %q, want %q", got[0], "alpha")
	}
	if len(got[1]) != maxColumnSampleValueLength {
		t.Fatalf("truncated value length = %d, want %d", len(got[1]), maxColumnSampleValueLength)
	}
	if !strings.HasSuffix(got[1], "...") {
		t.Fatalf("truncated value should end with ellipsis, got %q", got[1])
	}
}

func TestInt64FromDBValue(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
		want  int64
	}{
		{name: "int64", value: int64(42), want: 42},
		{name: "float string", value: "42.0", want: 42},
		{name: "byte string", value: []byte("7"), want: 7},
		{name: "nil", value: nil, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := int64FromDBValue(tt.value)
			if err != nil {
				t.Fatalf("int64FromDBValue(...) error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("int64FromDBValue(%v) = %d, want %d", tt.value, got, tt.want)
			}
		})
	}
}

func TestShouldSkipColumnSamples(t *testing.T) {
	if !shouldSkipColumnSamples("VECTOR(FLOAT, 768)") {
		t.Fatalf("vector columns should skip sample values")
	}
	if shouldSkipColumnSamples("VARCHAR") {
		t.Fatalf("non-vector columns should not skip sample values")
	}
}

func TestQuoteMySQLIdentifier(t *testing.T) {
	got := quoteMySQLIdentifier("orders")
	if got != "`orders`" {
		t.Fatalf("quoteMySQLIdentifier(simple) = %q, want %q", got, "`orders`")
	}

	got = quoteMySQLIdentifier("order`items")
	if got != "`order``items`" {
		t.Fatalf("quoteMySQLIdentifier(escaped) = %q, want %q", got, "`order``items`")
	}
}

func TestBuildMySQLDSN_DefaultPortAndParseTime(t *testing.T) {
	cfg := DatabaseConfig{
		Host:     "localhost",
		User:     "app",
		Password: "pa:ss@word",
		Port:     0,
		TLS:      "true",
	}

	dsn := buildMySQLDSN(cfg, "analytics")
	parsed, err := mysqlDriver.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("ParseDSN() error = %v", err)
	}

	if parsed.User != "app" {
		t.Fatalf("dsn user = %q, want %q", parsed.User, "app")
	}
	if parsed.Passwd != "pa:ss@word" {
		t.Fatalf("dsn password = %q, want %q", parsed.Passwd, "pa:ss@word")
	}
	if parsed.Addr != "localhost:3306" {
		t.Fatalf("dsn addr = %q, want %q", parsed.Addr, "localhost:3306")
	}
	if parsed.DBName != "analytics" {
		t.Fatalf("dsn database = %q, want %q", parsed.DBName, "analytics")
	}
	if !parsed.ParseTime {
		t.Fatalf("dsn parseTime = false, want true")
	}
	if parsed.TLSConfig != "true" {
		t.Fatalf("dsn tls = %q, want %q", parsed.TLSConfig, "true")
	}
}
