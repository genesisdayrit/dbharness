package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"

	gcpbigquery "cloud.google.com/go/bigquery"
	bigqueryv2 "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type bigQueryDiscoverer struct {
	client    *gcpbigquery.Client
	projectID string

	locationMu       sync.Mutex
	datasetLocations map[string]string
}

type bigQueryDatabaseLister struct {
	client        *gcpbigquery.Client
	service       *bigqueryv2.Service
	seedProjectID string
}

func newBigQuery(cfg DatabaseConfig) (*bigQueryDiscoverer, error) {
	projectID := resolveBigQueryProjectID(cfg)
	if projectID == "" {
		return nil, fmt.Errorf("bigquery requires project_id or database (project)")
	}

	clientOptions := bigQueryClientOptions(cfg)
	client, err := gcpbigquery.NewClient(context.Background(), projectID, clientOptions...)
	if err != nil {
		return nil, fmt.Errorf("open bigquery client: %w", err)
	}

	return &bigQueryDiscoverer{
		client:           client,
		projectID:        projectID,
		datasetLocations: make(map[string]string),
	}, nil
}

func newBigQueryDatabaseLister(cfg DatabaseConfig) (*bigQueryDatabaseLister, error) {
	seedProjectID := resolveBigQuerySeedProjectID(cfg)
	if seedProjectID == "" {
		return nil, fmt.Errorf("bigquery requires project_id or database (project)")
	}

	clientOptions := bigQueryClientOptions(cfg)
	client, err := gcpbigquery.NewClient(context.Background(), seedProjectID, clientOptions...)
	if err != nil {
		return nil, fmt.Errorf("open bigquery client: %w", err)
	}

	service, err := bigqueryv2.NewService(context.Background(), clientOptions...)
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("open bigquery api service: %w", err)
	}

	return &bigQueryDatabaseLister{
		client:        client,
		service:       service,
		seedProjectID: seedProjectID,
	}, nil
}

func resolveBigQueryProjectID(cfg DatabaseConfig) string {
	if projectID := strings.TrimSpace(cfg.Database); projectID != "" {
		return projectID
	}
	return strings.TrimSpace(cfg.ProjectID)
}

func resolveBigQuerySeedProjectID(cfg DatabaseConfig) string {
	if projectID := strings.TrimSpace(cfg.ProjectID); projectID != "" {
		return projectID
	}
	return strings.TrimSpace(cfg.Database)
}

func bigQueryClientOptions(cfg DatabaseConfig) []option.ClientOption {
	var clientOptions []option.ClientOption
	if credentialsFile := strings.TrimSpace(cfg.CredentialsFile); credentialsFile != "" {
		clientOptions = append(clientOptions, option.WithCredentialsFile(credentialsFile))
	}
	return clientOptions
}

func (b *bigQueryDatabaseLister) ListDatabases(ctx context.Context) ([]string, error) {
	seen := make(map[string]bool)
	var databases []string

	err := b.service.Projects.List().MaxResults(1000).Pages(ctx, func(page *bigqueryv2.ProjectList) error {
		for _, item := range page.Projects {
			if item == nil {
				continue
			}

			projectID := ""
			if item.ProjectReference != nil {
				projectID = strings.TrimSpace(item.ProjectReference.ProjectId)
			}
			if projectID == "" {
				projectID = strings.TrimSpace(item.Id)
			}
			if projectID == "" || seen[projectID] {
				continue
			}

			seen[projectID] = true
			databases = append(databases, projectID)
		}
		return nil
	})
	if err != nil {
		if fallback := strings.TrimSpace(b.seedProjectID); fallback != "" {
			return []string{fallback}, nil
		}
		return nil, fmt.Errorf("query bigquery projects: %w", err)
	}

	if len(databases) == 0 {
		if fallback := strings.TrimSpace(b.seedProjectID); fallback != "" {
			databases = append(databases, fallback)
		}
	}

	sort.Strings(databases)
	return databases, nil
}

func (b *bigQueryDatabaseLister) Close() error {
	return b.client.Close()
}

func (b *bigQueryDiscoverer) Discover(ctx context.Context) ([]SchemaInfo, error) {
	datasets, err := b.getDatasets(ctx)
	if err != nil {
		return nil, err
	}

	schemas := make([]SchemaInfo, 0, len(datasets))
	for _, dataset := range datasets {
		tables, err := b.getTables(ctx, dataset)
		if err != nil {
			return nil, fmt.Errorf("get tables for dataset %q: %w", dataset, err)
		}
		schemas = append(schemas, SchemaInfo{
			Name:   dataset,
			Tables: tables,
		})
	}

	return schemas, nil
}

func (b *bigQueryDiscoverer) getDatasets(ctx context.Context) ([]string, error) {
	it := b.client.DatasetsInProject(ctx, b.projectID)

	var datasets []string
	for {
		dataset, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("query bigquery datasets: %w", err)
		}

		name := strings.TrimSpace(dataset.DatasetID)
		if name == "" || isBigQuerySystemSchema(name) {
			continue
		}
		datasets = append(datasets, name)
	}

	sort.Strings(datasets)
	return datasets, nil
}

func isBigQuerySystemSchema(schemaName string) bool {
	switch strings.ToUpper(strings.TrimSpace(schemaName)) {
	case "INFORMATION_SCHEMA", "_SESSION", "_SCRIPT":
		return true
	default:
		return false
	}
}

func (b *bigQueryDiscoverer) getTables(ctx context.Context, dataset string) ([]TableInfo, error) {
	it := b.client.DatasetInProject(b.projectID, dataset).Tables(ctx)

	var tables []TableInfo
	for {
		table, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("query bigquery tables: %w", err)
		}

		metadata, err := table.Metadata(ctx)
		if err != nil {
			return nil, fmt.Errorf("read metadata for table %q: %w", table.TableID, err)
		}

		tables = append(tables, TableInfo{
			Name:      table.TableID,
			TableType: normalizeBigQueryTableType(metadata.Type),
		})
	}

	sort.Slice(tables, func(i, j int) bool {
		return tables[i].Name < tables[j].Name
	})
	return tables, nil
}

func normalizeBigQueryTableType(tableType gcpbigquery.TableType) string {
	switch tableType {
	case gcpbigquery.RegularTable:
		return "BASE TABLE"
	case gcpbigquery.ViewTable:
		return "VIEW"
	case gcpbigquery.MaterializedView:
		return "MATERIALIZED VIEW"
	case gcpbigquery.ExternalTable:
		return "EXTERNAL TABLE"
	case gcpbigquery.Snapshot:
		return "SNAPSHOT"
	default:
		normalized := strings.ToUpper(strings.TrimSpace(string(tableType)))
		if normalized == "" {
			return "BASE TABLE"
		}
		return strings.ReplaceAll(normalized, "_", " ")
	}
}

func (b *bigQueryDiscoverer) GetColumns(ctx context.Context, schema, table string) ([]ColumnInfo, error) {
	tableMetadata, err := b.client.DatasetInProject(b.projectID, schema).Table(table).Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("query bigquery columns metadata: %w", err)
	}

	columns := make([]ColumnInfo, 0, len(tableMetadata.Schema))
	for idx, field := range tableMetadata.Schema {
		columns = append(columns, bigQueryColumnInfoFromField(field, idx+1))
	}

	return columns, nil
}

func bigQueryColumnInfoFromField(field *gcpbigquery.FieldSchema, ordinal int) ColumnInfo {
	if field == nil {
		return ColumnInfo{OrdinalPosition: ordinal}
	}

	isNullable := "YES"
	if field.Required && !field.Repeated {
		isNullable = "NO"
	}

	return ColumnInfo{
		Name:            field.Name,
		DataType:        bigQueryFieldDataType(field),
		IsNullable:      isNullable,
		OrdinalPosition: ordinal,
		ColumnDefault:   strings.TrimSpace(field.DefaultValueExpression),
	}
}

func bigQueryFieldDataType(field *gcpbigquery.FieldSchema) string {
	if field == nil {
		return ""
	}

	baseType := strings.ToUpper(strings.TrimSpace(string(field.Type)))
	if field.Type == gcpbigquery.RecordFieldType {
		baseType = bigQueryStructType(field.Schema)
	}

	if field.Repeated {
		return fmt.Sprintf("ARRAY<%s>", baseType)
	}

	return baseType
}

func bigQueryStructType(schema gcpbigquery.Schema) string {
	if len(schema) == 0 {
		return "STRUCT"
	}

	parts := make([]string, 0, len(schema))
	for _, field := range schema {
		if field == nil {
			continue
		}
		fieldType := bigQueryFieldDataType(field)
		if fieldType == "" {
			fieldType = "STRING"
		}
		parts = append(parts, fmt.Sprintf("%s %s", field.Name, fieldType))
	}

	if len(parts) == 0 {
		return "STRUCT"
	}
	return fmt.Sprintf("STRUCT<%s>", strings.Join(parts, ", "))
}

func (b *bigQueryDiscoverer) GetColumnEnrichment(ctx context.Context, schema, table string, column ColumnInfo) (EnrichedColumnInfo, error) {
	profile := newEnrichedColumnInfo(column)

	quotedTable := quoteBigQueryTableReference(b.projectID, schema, table)
	quotedColumn := quoteBigQueryColumnPath(column.Name)

	statsQuery := fmt.Sprintf(`
		SELECT
			COUNT(1) AS total_rows,
			COUNTIF(%[1]s IS NULL) AS null_count,
			COUNTIF(%[1]s IS NOT NULL) AS non_null_count,
			COUNT(DISTINCT IF(%[1]s IS NULL, NULL, TO_JSON_STRING(%[1]s))) AS distinct_non_null_count
		FROM %[2]s
	`, quotedColumn, quotedTable)

	stats, err := b.readSingleRow(ctx, schema, statsQuery)
	if err != nil {
		return EnrichedColumnInfo{}, fmt.Errorf(
			"profile bigquery column %q on %s.%s: %w",
			column.Name,
			schema,
			table,
			err,
		)
	}
	if len(stats) < 4 {
		return EnrichedColumnInfo{}, fmt.Errorf(
			"profile bigquery column %q on %s.%s: expected 4 stats values, got %d",
			column.Name,
			schema,
			table,
			len(stats),
		)
	}

	profile.TotalRows, err = int64FromDBValue(stats[0])
	if err != nil {
		return EnrichedColumnInfo{}, fmt.Errorf("parse total_rows for %q on %s.%s: %w", column.Name, schema, table, err)
	}
	profile.NullCount, err = int64FromDBValue(stats[1])
	if err != nil {
		return EnrichedColumnInfo{}, fmt.Errorf("parse null_count for %q on %s.%s: %w", column.Name, schema, table, err)
	}
	profile.NonNullCount, err = int64FromDBValue(stats[2])
	if err != nil {
		return EnrichedColumnInfo{}, fmt.Errorf("parse non_null_count for %q on %s.%s: %w", column.Name, schema, table, err)
	}
	profile.DistinctNonNullCount, err = int64FromDBValue(stats[3])
	if err != nil {
		return EnrichedColumnInfo{}, fmt.Errorf("parse distinct_non_null_count for %q on %s.%s: %w", column.Name, schema, table, err)
	}

	profile.DistinctOfNonNullPct = percentOfTotal(profile.DistinctNonNullCount, profile.NonNullCount)
	profile.NullOfTotalRowsPct = percentOfTotal(profile.NullCount, profile.TotalRows)
	profile.NonNullOfTotalRowsPct = percentOfTotal(profile.NonNullCount, profile.TotalRows)

	if shouldSkipColumnSamples(column.DataType) {
		return profile, nil
	}

	sampleQuery := fmt.Sprintf(`
		SELECT DISTINCT LEFT(TO_JSON_STRING(%[1]s), %[2]d)
		FROM %[3]s
		WHERE %[1]s IS NOT NULL
		LIMIT %[4]d
	`, quotedColumn, maxColumnSampleValueLength, quotedTable, columnProfileSampleValueLimit)

	samples, err := b.readSingleColumnValues(ctx, schema, sampleQuery)
	if err != nil {
		return EnrichedColumnInfo{}, fmt.Errorf(
			"query bigquery sample values for %q on %s.%s: %w",
			column.Name,
			schema,
			table,
			err,
		)
	}

	profile.SampleValues = normalizeColumnSampleValues(samples)
	return profile, nil
}

func (b *bigQueryDiscoverer) GetSampleRows(ctx context.Context, schema, table string, limit int) (*SampleResult, error) {
	if limit <= 0 {
		limit = 10
	}

	query := fmt.Sprintf(
		"SELECT * FROM %s ORDER BY RAND() LIMIT %d",
		quoteBigQueryTableReference(b.projectID, schema, table),
		limit,
	)

	it, err := b.runQuery(ctx, schema, query)
	if err != nil {
		return nil, fmt.Errorf("query bigquery sample rows: %w", err)
	}

	return scanBigQuerySampleRows(it)
}

func (b *bigQueryDiscoverer) readSingleRow(ctx context.Context, dataset, queryText string) ([]gcpbigquery.Value, error) {
	it, err := b.runQuery(ctx, dataset, queryText)
	if err != nil {
		return nil, err
	}

	var row []gcpbigquery.Value
	if err := it.Next(&row); err != nil {
		if err == iterator.Done {
			return nil, fmt.Errorf("query returned no rows")
		}
		return nil, fmt.Errorf("scan query row: %w", err)
	}
	return row, nil
}

func (b *bigQueryDiscoverer) readSingleColumnValues(ctx context.Context, dataset, queryText string) ([]string, error) {
	it, err := b.runQuery(ctx, dataset, queryText)
	if err != nil {
		return nil, err
	}

	var values []string
	for {
		var row []gcpbigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			return nil, fmt.Errorf("scan query row: %w", err)
		}
		if len(row) == 0 {
			continue
		}
		values = append(values, formatBigQueryValue(row[0]))
	}

	return values, nil
}

func (b *bigQueryDiscoverer) runQuery(ctx context.Context, dataset, queryText string) (*gcpbigquery.RowIterator, error) {
	query := b.client.Query(queryText)
	if location := b.datasetLocation(ctx, dataset); location != "" {
		query.Location = location
	}

	it, err := query.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("run bigquery query: %w", err)
	}
	return it, nil
}

func (b *bigQueryDiscoverer) datasetLocation(ctx context.Context, dataset string) string {
	dataset = strings.TrimSpace(dataset)
	if dataset == "" {
		return ""
	}

	b.locationMu.Lock()
	if location, ok := b.datasetLocations[dataset]; ok {
		b.locationMu.Unlock()
		return location
	}
	b.locationMu.Unlock()

	metadata, err := b.client.DatasetInProject(b.projectID, dataset).Metadata(ctx)
	if err != nil {
		return ""
	}
	location := strings.TrimSpace(metadata.Location)

	b.locationMu.Lock()
	b.datasetLocations[dataset] = location
	b.locationMu.Unlock()
	return location
}

func scanBigQuerySampleRows(it *gcpbigquery.RowIterator) (*SampleResult, error) {
	result := &SampleResult{}

	for {
		var row []gcpbigquery.Value
		if err := it.Next(&row); err != nil {
			if err == iterator.Done {
				break
			}
			return nil, fmt.Errorf("scan sample row: %w", err)
		}

		if len(result.Columns) == 0 {
			result.Columns = bigQueryColumnNames(it.Schema, len(row))
		}

		formattedRow := make([]string, len(row))
		for i, value := range row {
			formattedRow[i] = formatBigQueryValue(value)
		}
		result.Rows = append(result.Rows, formattedRow)
	}

	if len(result.Columns) == 0 {
		result.Columns = bigQueryColumnNames(it.Schema, 0)
	}

	return result, nil
}

func bigQueryColumnNames(schema gcpbigquery.Schema, fallbackCount int) []string {
	if len(schema) > 0 {
		columns := make([]string, len(schema))
		for i, field := range schema {
			columnName := ""
			if field != nil {
				columnName = strings.TrimSpace(field.Name)
			}
			if columnName == "" {
				columnName = fmt.Sprintf("column_%d", i+1)
			}
			columns[i] = columnName
		}
		return columns
	}

	if fallbackCount <= 0 {
		return nil
	}

	columns := make([]string, fallbackCount)
	for i := 0; i < fallbackCount; i++ {
		columns[i] = fmt.Sprintf("column_%d", i+1)
	}
	return columns
}

func quoteBigQueryTableReference(projectID, datasetID, tableID string) string {
	fullyQualified := strings.Join([]string{
		strings.TrimSpace(projectID),
		strings.TrimSpace(datasetID),
		strings.TrimSpace(tableID),
	}, ".")
	return quoteBigQueryIdentifier(fullyQualified)
}

func quoteBigQueryColumnPath(columnPath string) string {
	parts := strings.Split(strings.TrimSpace(columnPath), ".")
	quoted := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		quoted = append(quoted, quoteBigQueryIdentifier(part))
	}

	if len(quoted) == 0 {
		return quoteBigQueryIdentifier(columnPath)
	}
	return strings.Join(quoted, ".")
}

func formatBigQueryValue(value interface{}) string {
	normalized := normalizeBigQueryValue(value)
	if normalized == nil {
		return ""
	}

	switch v := normalized.(type) {
	case string:
		return v
	case bool, int, int32, int64, float32, float64:
		return formatValue(v)
	default:
		data, err := json.Marshal(v)
		if err == nil {
			return string(data)
		}
		return formatValue(v)
	}
}

func normalizeBigQueryValue(value interface{}) interface{} {
	switch v := value.(type) {
	case nil:
		return nil
	case string:
		return v
	case []byte:
		return string(v)
	case bool, int, int32, int64, float32, float64:
		return v
	case gcpbigquery.NullString:
		if v.Valid {
			return v.StringVal
		}
		return nil
	case gcpbigquery.NullInt64:
		if v.Valid {
			return v.Int64
		}
		return nil
	case gcpbigquery.NullFloat64:
		if v.Valid {
			return v.Float64
		}
		return nil
	case gcpbigquery.NullBool:
		if v.Valid {
			return v.Bool
		}
		return nil
	case gcpbigquery.NullTimestamp:
		if v.Valid {
			return formatValue(v.Timestamp)
		}
		return nil
	case gcpbigquery.NullDate:
		if v.Valid {
			return v.Date.String()
		}
		return nil
	case gcpbigquery.NullTime:
		if v.Valid {
			return v.Time.String()
		}
		return nil
	case gcpbigquery.NullDateTime:
		if v.Valid {
			return v.DateTime.String()
		}
		return nil
	case gcpbigquery.NullJSON:
		if v.Valid {
			return v.JSONVal
		}
		return nil
	case gcpbigquery.NullGeography:
		if v.Valid {
			return v.GeographyVal
		}
		return nil
	case []gcpbigquery.Value:
		out := make([]interface{}, len(v))
		for i, item := range v {
			out[i] = normalizeBigQueryValue(item)
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(v))
		for i, item := range v {
			out[i] = normalizeBigQueryValue(item)
		}
		return out
	case map[string]gcpbigquery.Value:
		out := make(map[string]interface{}, len(v))
		for key, item := range v {
			out[key] = normalizeBigQueryValue(item)
		}
		return out
	case map[string]interface{}:
		out := make(map[string]interface{}, len(v))
		for key, item := range v {
			out[key] = normalizeBigQueryValue(item)
		}
		return out
	default:
		return formatValue(v)
	}
}

func (b *bigQueryDiscoverer) Close() error {
	return b.client.Close()
}
