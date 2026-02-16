package discovery

import (
	"database/sql"
	"encoding/json"
	"testing"
	"time"
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
