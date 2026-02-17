package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/charmbracelet/huh"
	"github.com/genesisdayrit/dbharness/internal/contextgen"
	"github.com/genesisdayrit/dbharness/internal/discovery"
	"github.com/genesisdayrit/dbharness/internal/template"
	_ "github.com/lib/pq"
	"github.com/snowflakedb/gosnowflake"
	"gopkg.in/yaml.v3"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	switch os.Args[1] {
	case "init":
		runInit(os.Args[2:])
	case "test-connection":
		runTestConnection(os.Args[2:])
	case "snapshot":
		runSnapshot(os.Args[2:])
	case "ls":
		runList(os.Args[2:])
	case "set-default":
		runSetDefault(os.Args[2:])
	case "schemas":
		runSchemas(os.Args[2:])
	case "tables":
		runTables(os.Args[2:])
	case "columns":
		runColumns(os.Args[2:])
	case "update-databases":
		runUpdateDatabases(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "Usage:")
	fmt.Fprintln(os.Stderr, "  dbh init [--force]")
	fmt.Fprintln(os.Stderr, "  dbh test-connection [-s name]")
	fmt.Fprintln(os.Stderr, "  dbh snapshot")
	fmt.Fprintln(os.Stderr, "  dbh snapshot config")
	fmt.Fprintln(os.Stderr, "  dbh ls -c")
	fmt.Fprintln(os.Stderr, "  dbh set-default -c")
	fmt.Fprintln(os.Stderr, "  dbh set-default -d")
	fmt.Fprintln(os.Stderr, "  dbh schemas [-s name]")
	fmt.Fprintln(os.Stderr, "  dbh tables [-s name]")
	fmt.Fprintln(os.Stderr, "  dbh columns [-s name]")
	fmt.Fprintln(os.Stderr, "  dbh update-databases [-s name]")
}

func runInit(args []string) {
	flags := flag.NewFlagSet("init", flag.ExitOnError)
	force := flags.Bool("force", false, "Overwrite an existing .dbharness folder.")
	_ = flags.Parse(args)

	targetDir := filepath.Join(".", ".dbharness")

	if info, err := os.Stat(targetDir); err == nil && info.IsDir() && !*force {
		absPath, _ := filepath.Abs(targetDir)
		fmt.Printf(".dbharness already exists at %s\n", absPath)
		fmt.Println()
		if promptYesNo("Would you like to add a new connection?") {
			addConnectionEntry(targetDir, false)
		} else {
			fmt.Println("No changes made.")
		}
		return
	}

	snapshotPath, err := installTemplate(targetDir, *force)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if snapshotPath != "" {
		absSnapshotPath, _ := filepath.Abs(snapshotPath)
		fmt.Printf("Snapshot saved to %s\n", absSnapshotPath)
	}

	absPath, _ := filepath.Abs(targetDir)
	fmt.Printf("Installed .dbharness to %s\n", absPath)
	fmt.Println()
	addConnectionEntry(targetDir, true)
}

type config struct {
	Connections []databaseConfig `json:"connections"`
}

type databaseConfig struct {
	Name        string `json:"name"`
	Environment string `json:"environment,omitempty"`
	Type        string `json:"type"`
	Primary     bool   `json:"primary"`

	// Shared
	Database string `json:"database,omitempty"`
	User     string `json:"user"`

	// Postgres-specific
	Host     string `json:"host,omitempty"`
	Port     int    `json:"port,omitempty"`
	Password string `json:"password,omitempty"`
	SSLMode  string `json:"sslmode,omitempty"`

	// Snowflake-specific
	Account       string `json:"account,omitempty"`
	Role          string `json:"role,omitempty"`
	Warehouse     string `json:"warehouse,omitempty"`
	Schema        string `json:"schema,omitempty"`
	Authenticator string `json:"authenticator,omitempty"`
}

func runTestConnection(args []string) {
	flags := flag.NewFlagSet("test-connection", flag.ExitOnError)
	shortName := flags.String("s", "", "Database name from config.json (default: \"default\").")
	longName := flags.String("name", "", "Database name from config.json (default: \"default\").")
	_ = flags.Parse(args)

	name := *shortName
	if name == "" {
		name = *longName
	}
	if name == "" {
		name = "default"
	}

	cfg, err := readConfig(filepath.Join(".", ".dbharness", "config.json"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	dbConfig, err := findDatabaseConfig(cfg, name)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if err := pingDatabase(dbConfig); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	fmt.Printf("Connection ok: %s\n", dbConfig.Name)
}

func runSnapshot(args []string) {
	flags := flag.NewFlagSet("snapshot", flag.ExitOnError)
	_ = flags.Parse(args)

	configOnly := flags.NArg() > 0 && flags.Arg(0) == "config"

	ensureGitignore()

	sourceDir := filepath.Join(".", ".dbharness")

	if configOnly {
		srcPath := filepath.Join(sourceDir, "config.json")
		data, err := os.ReadFile(srcPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "read config: %v\n", err)
			os.Exit(1)
		}
		snapshotDir, err := createSnapshotDir(sourceDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "create snapshot dir: %v\n", err)
			os.Exit(1)
		}
		destPath := filepath.Join(snapshotDir, "config.json")
		if err := os.WriteFile(destPath, data, 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "write snapshot: %v\n", err)
			os.Exit(1)
		}
		absPath, _ := filepath.Abs(destPath)
		fmt.Printf("Snapshot saved to %s\n", absPath)
	} else {
		snapshotDir, err := snapshotDirectory(sourceDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "snapshot: %v\n", err)
			os.Exit(1)
		}
		absPath, _ := filepath.Abs(snapshotDir)
		fmt.Printf("Snapshot saved to %s\n", absPath)
	}
}

func runList(args []string) {
	flags := flag.NewFlagSet("ls", flag.ExitOnError)
	shortConnections := flags.Bool("c", false, "List configured connections.")
	longConnections := flags.Bool("connections", false, "List configured connections.")
	_ = flags.Parse(args)

	if flags.NArg() > 0 {
		fmt.Fprintln(os.Stderr, "ls does not accept positional arguments")
		os.Exit(2)
	}

	if !*shortConnections && !*longConnections {
		fmt.Fprintln(os.Stderr, "ls requires -c or --connections")
		os.Exit(2)
	}

	cfg, err := readConfig(filepath.Join(".", ".dbharness", "config.json"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	printConnections(os.Stdout, cfg)
}

func runSetDefault(args []string) {
	flags := flag.NewFlagSet("set-default", flag.ExitOnError)
	shortConnections := flags.Bool("c", false, "Select and set the primary connection.")
	longConnections := flags.Bool("connections", false, "Select and set the primary connection.")
	shortDatabase := flags.Bool("d", false, "Select and set the default database for the primary connection using _databases.yml.")
	longDatabase := flags.Bool("database", false, "Select and set the default database for the primary connection using _databases.yml.")
	_ = flags.Parse(args)

	if flags.NArg() > 0 {
		fmt.Fprintln(os.Stderr, "set-default does not accept positional arguments")
		os.Exit(2)
	}

	setConnections := *shortConnections || *longConnections
	setDatabase := *shortDatabase || *longDatabase

	if setConnections == setDatabase {
		fmt.Fprintln(os.Stderr, "set-default requires exactly one of -c/--connections or -d/--database")
		os.Exit(2)
	}

	if setConnections {
		runSetDefaultConnection()
		return
	}

	runSetDefaultDatabase()
}

func runSetDefaultConnection() {
	configPath := filepath.Join(".", ".dbharness", "config.json")
	cfg, err := readConfig(configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if len(cfg.Connections) == 0 {
		fmt.Fprintln(os.Stderr, "no connections configured in config.json")
		os.Exit(1)
	}

	names := make([]string, 0, len(cfg.Connections))
	for _, conn := range cfg.Connections {
		names = append(names, conn.Name)
	}

	selected, err := promptSelectRequired("Select a primary connection", names)
	if err != nil {
		fmt.Fprintf(os.Stderr, "select primary connection: %v\n", err)
		os.Exit(1)
	}

	previousPrimary, changed, err := setPrimaryConnection(&cfg, selected)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if !changed {
		fmt.Printf("Connection %q is already the primary default.\n", selected)
		return
	}

	if err := writeConfig(configPath, cfg); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	absConfigPath, _ := filepath.Abs(configPath)
	if strings.TrimSpace(previousPrimary) == "" {
		fmt.Printf("Primary default connection set to %q in %s\n", selected, absConfigPath)
		return
	}

	fmt.Printf("Primary default connection switched from %q to %q in %s\n", previousPrimary, selected, absConfigPath)
}

const keepCurrentDefaultSelectionValue = "__keep_current_default_database__"

type databasesCatalog struct {
	DatabaseType    string
	DefaultDatabase string
	Databases       []string
}

func runSetDefaultDatabase() {
	baseDir := filepath.Join(".", ".dbharness")
	configPath := filepath.Join(baseDir, "config.json")
	cfg, err := readConfig(configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	primary, err := findPrimaryConnection(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	databasesPath := filepath.Join(baseDir, "context", "connections", primary.Name, "databases", "_databases.yml")
	catalog, err := readDatabasesCatalog(databasesPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			absDatabasesPath, _ := filepath.Abs(databasesPath)
			fmt.Fprintf(
				os.Stderr,
				"could not read %s: run \"dbh update-databases -s %s\" first to create it\n",
				absDatabasesPath,
				primary.Name,
			)
			os.Exit(1)
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if len(catalog.Databases) == 0 {
		fmt.Fprintf(os.Stderr, "no databases listed in %s\n", databasesPath)
		os.Exit(1)
	}

	currentDefault := resolveCurrentDefaultDatabase(primary.Database, catalog.DefaultDatabase)
	if currentDefault == "" {
		fmt.Printf("No default database is currently configured for connection %q.\n", primary.Name)
	} else {
		fmt.Printf("Current default database for connection %q: %q\n", primary.Name, currentDefault)
	}

	selected, err := promptSelectDefaultDatabase(currentDefault, catalog.Databases)
	if err != nil {
		fmt.Fprintf(os.Stderr, "select default database: %v\n", err)
		os.Exit(1)
	}

	if selected == keepCurrentDefaultSelectionValue {
		fmt.Println("Keeping existing default database.")
		return
	}

	selected = strings.TrimSpace(selected)
	if selected == "" {
		fmt.Fprintln(os.Stderr, "selected default database cannot be empty")
		os.Exit(1)
	}

	configNeedsUpdate := strings.TrimSpace(primary.Database) != selected
	databasesNeedsUpdate := strings.TrimSpace(catalog.DefaultDatabase) != selected

	if configNeedsUpdate {
		updated, err := setConnectionDefaultDatabase(&cfg, primary.Name, selected)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if updated {
			if err := writeConfig(configPath, cfg); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		}
	}

	if databasesNeedsUpdate {
		databaseType := strings.TrimSpace(catalog.DatabaseType)
		if databaseType == "" {
			databaseType = primary.Type
		}
		if err := writeDefaultDatabaseToDatabasesFile(baseDir, primary.Name, databaseType, selected, catalog.Databases); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}

	if !configNeedsUpdate && !databasesNeedsUpdate {
		fmt.Printf("Default database for connection %q is already %q.\n", primary.Name, selected)
		return
	}

	absConfigPath, _ := filepath.Abs(configPath)
	absDatabasesPath, _ := filepath.Abs(databasesPath)
	if configNeedsUpdate {
		fmt.Printf("Updated default database to %q in %s\n", selected, absConfigPath)
	}
	if databasesNeedsUpdate {
		fmt.Printf("Updated default database to %q in %s\n", selected, absDatabasesPath)
	}
}

func resolveCurrentDefaultDatabase(configDefault, fileDefault string) string {
	if current := strings.TrimSpace(configDefault); current != "" {
		return current
	}
	fileDefault = strings.TrimSpace(fileDefault)
	if fileDefault == "" || fileDefault == "_default" {
		return ""
	}
	return fileDefault
}

func promptSelectDefaultDatabase(currentDefault string, databases []string) (string, error) {
	databases = normalizeDatabaseNames(databases)
	if len(databases) == 0 {
		return "", fmt.Errorf("no databases available to select")
	}

	options := make([]huh.Option[string], 0, len(databases)+1)
	if strings.TrimSpace(currentDefault) != "" {
		label := fmt.Sprintf("Keep current default database (%s)", currentDefault)
		options = append(options, huh.NewOption(label, keepCurrentDefaultSelectionValue))
	}
	for _, database := range databases {
		options = append(options, huh.NewOption(database, database))
	}

	var selected string
	if err := huh.NewSelect[string]().
		Title("Select a default database").
		Options(options...).
		Value(&selected).
		Run(); err != nil {
		return "", err
	}

	selected = strings.TrimSpace(selected)
	if selected == "" {
		return "", fmt.Errorf("no option selected")
	}
	return selected, nil
}

func readDatabasesCatalog(path string) (databasesCatalog, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return databasesCatalog{}, fmt.Errorf("read _databases.yml: %w", err)
	}

	var file contextgen.DatabasesFile
	if err := yaml.Unmarshal(data, &file); err != nil {
		return databasesCatalog{}, fmt.Errorf("parse _databases.yml: %w", err)
	}

	names := make([]string, 0, len(file.Databases))
	for _, item := range file.Databases {
		names = append(names, item.Name)
	}

	return databasesCatalog{
		DatabaseType:    strings.TrimSpace(file.DatabaseType),
		DefaultDatabase: strings.TrimSpace(file.DefaultDatabase),
		Databases:       normalizeDatabaseNames(names),
	}, nil
}

func writeDefaultDatabaseToDatabasesFile(baseDir, connectionName, databaseType, defaultDatabase string, databases []string) error {
	opts := contextgen.Options{
		ConnectionName: connectionName,
		DatabaseName:   defaultDatabase,
		DatabaseType:   databaseType,
		BaseDir:        baseDir,
	}

	if _, err := contextgen.UpdateDatabasesFile(databases, opts); err != nil {
		return fmt.Errorf("update _databases.yml: %w", err)
	}
	return nil
}

func runSchemas(args []string) {
	flags := flag.NewFlagSet("schemas", flag.ExitOnError)
	shortName := flags.String("s", "", "Connection name from config.json.")
	longName := flags.String("name", "", "Connection name from config.json.")
	_ = flags.Parse(args)

	name := *shortName
	if name == "" {
		name = *longName
	}

	baseDir := filepath.Join(".", ".dbharness")
	configPath := filepath.Join(baseDir, "config.json")
	cfg, err := readConfig(configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	// If no name provided, use the primary connection or the first one.
	var dbCfg databaseConfig
	if name == "" {
		dbCfg, err = findPrimaryConnection(cfg)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	} else {
		dbCfg, err = findDatabaseConfig(cfg, name)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}

	if err := ensureDefaultDatabaseForSchemas(&cfg, &dbCfg, configPath); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	fmt.Printf("Discovering schemas for connection %q (%s)...\n", dbCfg.Name, dbCfg.Type)
	if dbCfg.Type == "snowflake" && dbCfg.Authenticator == "externalbrowser" {
		fmt.Println("Opening browser for SSO authentication...")
	}

	discoveryCfg := discovery.DatabaseConfig{
		Type:          dbCfg.Type,
		Database:      dbCfg.Database,
		Host:          dbCfg.Host,
		Port:          dbCfg.Port,
		User:          dbCfg.User,
		Password:      dbCfg.Password,
		SSLMode:       dbCfg.SSLMode,
		Account:       dbCfg.Account,
		Role:          dbCfg.Role,
		Warehouse:     dbCfg.Warehouse,
		Schema:        dbCfg.Schema,
		Authenticator: dbCfg.Authenticator,
	}

	disc, err := discovery.New(discoveryCfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect: %v\n", err)
		os.Exit(1)
	}
	defer disc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	schemas, err := disc.Discover(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "discover schemas: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Found %d schema(s)\n", len(schemas))

	totalTables := 0
	for _, s := range schemas {
		totalTables += len(s.Tables)
		fmt.Printf("  %-30s %d table(s)\n", s.Name, len(s.Tables))
	}
	fmt.Printf("Total: %d table(s) across %d schema(s)\n", totalTables, len(schemas))
	fmt.Println()

	opts := contextgen.Options{
		ConnectionName: dbCfg.Name,
		DatabaseName:   dbCfg.Database,
		DatabaseType:   dbCfg.Type,
		BaseDir:        baseDir,
	}

	if err := contextgen.Generate(schemas, opts); err != nil {
		fmt.Fprintf(os.Stderr, "generate context files: %v\n", err)
		os.Exit(1)
	}

	dbName := sanitizeSchemaName(dbCfg.Database)
	if dbName == "" {
		dbName = "_default"
	}

	databasesDir := filepath.Join(baseDir, "context", "connections", dbCfg.Name, "databases")
	schemasDir := filepath.Join(databasesDir, dbName, "schemas")
	absPath, _ := filepath.Abs(schemasDir)
	fmt.Printf("Schema context files written to %s\n", absPath)
	fmt.Println()
	fmt.Println("Files generated:")
	fmt.Printf("  %s/_databases.yml\n", databasesDir)
	fmt.Printf("  %s/_schemas.yml\n", schemasDir)
	for _, s := range schemas {
		fmt.Printf("  %s/%s/_tables.yml\n", schemasDir, sanitizeSchemaName(s.Name))
	}
}

func runTables(args []string) {
	flags := flag.NewFlagSet("tables", flag.ExitOnError)
	shortName := flags.String("s", "", "Connection name from config.json.")
	longName := flags.String("name", "", "Connection name from config.json.")
	_ = flags.Parse(args)

	name := *shortName
	if name == "" {
		name = *longName
	}

	baseDir := filepath.Join(".", ".dbharness")
	configPath := filepath.Join(baseDir, "config.json")
	cfg, err := readConfig(configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	var dbCfg databaseConfig
	if name == "" {
		dbCfg, err = findPrimaryConnection(cfg)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	} else {
		dbCfg, err = findDatabaseConfig(cfg, name)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}

	fmt.Printf("Using connection %q (%s)\n\n", dbCfg.Name, dbCfg.Type)

	// --- Database selection ---
	selectedDatabases, err := selectDatabasesForTables(&cfg, &dbCfg, configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if len(selectedDatabases) == 0 {
		fmt.Println("No databases selected.")
		return
	}

	for _, database := range selectedDatabases {
		fmt.Printf("\n--- Database: %s ---\n", database)

		dbCfgCopy := dbCfg
		dbCfgCopy.Database = database

		processDatabase(dbCfgCopy, baseDir, database)
	}
}

const (
	minSecondsPerColumnEstimate = 5
	maxSecondsPerColumnEstimate = 10
	columnMetadataTimeout       = 60 * time.Second
	columnEnrichmentTimeout     = 2 * time.Minute
)

type tableColumnTarget struct {
	Schema  string
	Table   string
	Columns []discovery.ColumnInfo
}

func runColumns(args []string) {
	flags := flag.NewFlagSet("columns", flag.ExitOnError)
	shortName := flags.String("s", "", "Connection name from config.json.")
	longName := flags.String("name", "", "Connection name from config.json.")
	_ = flags.Parse(args)

	name := *shortName
	if name == "" {
		name = *longName
	}

	baseDir := filepath.Join(".", ".dbharness")
	configPath := filepath.Join(baseDir, "config.json")
	cfg, err := readConfig(configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	var dbCfg databaseConfig
	if name == "" {
		dbCfg, err = findPrimaryConnection(cfg)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	} else {
		dbCfg, err = findDatabaseConfig(cfg, name)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}

	fmt.Printf("Using connection %q (%s)\n\n", dbCfg.Name, dbCfg.Type)
	fmt.Println("Warning: dbh columns enriches each selected column and may take several minutes to complete.")
	if !promptYesNo("Continue with enriched column profiling?") {
		fmt.Println("Aborted.")
		return
	}
	fmt.Println()

	selectedDatabases, err := selectDatabasesForTables(&cfg, &dbCfg, configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if len(selectedDatabases) == 0 {
		fmt.Println("No databases selected.")
		return
	}

	for _, database := range selectedDatabases {
		fmt.Printf("\n--- Database: %s ---\n", database)

		dbCfgCopy := dbCfg
		dbCfgCopy.Database = database

		processDatabaseColumns(dbCfgCopy, baseDir, database)
	}
}

func processDatabaseColumns(dbCfg databaseConfig, baseDir, database string) {
	discoveryCfg := toDiscoveryConfig(dbCfg)

	if dbCfg.Type == "snowflake" && dbCfg.Authenticator == "externalbrowser" {
		fmt.Println("Opening browser for SSO authentication...")
	}

	disc, err := discovery.NewTableDetailDiscoverer(discoveryCfg)
	if err != nil {
		fmt.Printf("Could not connect to database %q: %v\n", database, err)
		return
	}
	defer disc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	schemas, err := discoverSchemasWithProgress(ctx, disc)
	if err != nil {
		fmt.Printf("Could not discover schemas for %q: %v\n", database, err)
		return
	}
	if len(schemas) == 0 {
		fmt.Println("No schemas found.")
		return
	}

	schemaNames := make([]string, len(schemas))
	for i, s := range schemas {
		schemaNames[i] = s.Name
	}
	sort.Strings(schemaNames)

	fmt.Printf("Found %d schema(s)\n\n", len(schemas))
	selectedSchemas, err := promptMultiSelectWithAll("Select schemas", schemaNames)
	if err != nil {
		fmt.Printf("Schema selection failed: %v\n", err)
		return
	}
	if len(selectedSchemas) == 0 {
		fmt.Println("No schemas selected.")
		return
	}

	selectedTables, selectedTableCount, err := selectTablesForColumns(schemas, selectedSchemas)
	if err != nil {
		fmt.Printf("Table selection failed: %v\n", err)
		return
	}
	if selectedTableCount == 0 {
		fmt.Println("No tables selected.")
		return
	}

	targets, skippedTargets := buildColumnEnrichmentTargets(disc, schemas, selectedTables)
	if len(targets) == 0 {
		fmt.Println("No tables with accessible columns to process.")
		return
	}

	totalColumns := 0
	for _, target := range targets {
		totalColumns += len(target.Columns)
	}
	if totalColumns == 0 {
		fmt.Println("No columns found for selected tables.")
		return
	}

	minEstimate := time.Duration(totalColumns*minSecondsPerColumnEstimate) * time.Second
	maxEstimate := time.Duration(totalColumns*maxSecondsPerColumnEstimate) * time.Second

	fmt.Printf(
		"Selected %d table(s) across %d schema(s) with %d total column(s).\n",
		len(targets),
		len(selectedTables),
		totalColumns,
	)
	fmt.Printf("Estimated runtime: %s to %s\n", minEstimate.Round(time.Second), maxEstimate.Round(time.Second))

	opts := contextgen.Options{
		ConnectionName: dbCfg.Name,
		DatabaseName:   database,
		DatabaseType:   dbCfg.Type,
		BaseDir:        baseDir,
	}

	startedAt := time.Now()
	processedColumns := 0
	writtenTables := 0
	skippedTables := skippedTargets

	for _, target := range targets {
		tableStart := time.Now()
		fmt.Printf("\nProcessing table %s.%s (%d column(s))...\n", target.Schema, target.Table, len(target.Columns))

		enrichedColumns := make([]discovery.EnrichedColumnInfo, 0, len(target.Columns))
		tableFailed := false

		for _, column := range target.Columns {
			columnStart := time.Now()

			columnCtx, columnCancel := context.WithTimeout(context.Background(), columnEnrichmentTimeout)
			profile, err := disc.GetColumnEnrichment(columnCtx, target.Schema, target.Table, column)
			columnCancel()
			if err != nil {
				tableFailed = true
				fmt.Printf(
					"  Failed profiling %s.%s.%s: %v\n",
					target.Schema,
					target.Table,
					column.Name,
					err,
				)
				break
			}

			enrichedColumns = append(enrichedColumns, profile)
			processedColumns++

			remaining := totalColumns - processedColumns
			remainingETA := estimateRemainingDuration(time.Since(startedAt), processedColumns, remaining)
			fmt.Printf(
				"  [%d/%d] %s.%s.%s profiled (%s, est. remaining %s)\n",
				processedColumns,
				totalColumns,
				target.Schema,
				target.Table,
				column.Name,
				time.Since(columnStart).Round(time.Millisecond),
				remainingETA,
			)
		}

		if tableFailed || len(enrichedColumns) != len(target.Columns) {
			skippedTables++
			fmt.Printf("  Skipping file write for %s.%s because not all columns were processed.\n", target.Schema, target.Table)
			continue
		}

		path, err := contextgen.WriteEnrichedColumnsFile(
			contextgen.EnrichedColumnsInput{
				Schema:  target.Schema,
				Table:   target.Table,
				Columns: enrichedColumns,
			},
			opts,
		)
		if err != nil {
			skippedTables++
			fmt.Printf("  Failed writing enriched columns file for %s.%s: %v\n", target.Schema, target.Table, err)
			continue
		}

		writtenTables++
		absPath, _ := filepath.Abs(path)
		fmt.Printf("  Wrote %s (%s)\n", absPath, time.Since(tableStart).Round(time.Millisecond))
	}

	fmt.Printf(
		"\nFinished enriched columns for database %q: wrote %d table file(s), skipped %d, processed %d/%d columns in %s.\n",
		database,
		writtenTables,
		skippedTables,
		processedColumns,
		totalColumns,
		time.Since(startedAt).Round(time.Second),
	)
}

func selectTablesForColumns(
	schemas []discovery.SchemaInfo,
	selectedSchemas []string,
) (map[string][]string, int, error) {
	selectedTables := make(map[string][]string, len(selectedSchemas))
	schemaByName := make(map[string]discovery.SchemaInfo, len(schemas))
	for _, schema := range schemas {
		schemaByName[schema.Name] = schema
	}

	sort.Strings(selectedSchemas)

	totalTables := 0
	for _, schemaName := range selectedSchemas {
		schema, ok := schemaByName[schemaName]
		if !ok {
			continue
		}

		tableNames := make([]string, 0, len(schema.Tables))
		for _, table := range schema.Tables {
			tableNames = append(tableNames, table.Name)
		}
		sort.Strings(tableNames)
		if len(tableNames) == 0 {
			fmt.Printf("Schema %q has no tables.\n", schemaName)
			continue
		}

		selected, err := promptMultiSelectWithAll(
			fmt.Sprintf("Select tables in schema %s", schemaName),
			tableNames,
		)
		if err != nil {
			return nil, 0, err
		}
		if len(selected) == 0 {
			fmt.Printf("No tables selected for schema %q.\n", schemaName)
			continue
		}

		selectedTables[schemaName] = selected
		totalTables += len(selected)
	}

	return selectedTables, totalTables, nil
}

func buildColumnEnrichmentTargets(
	disc discovery.TableDetailDiscoverer,
	schemas []discovery.SchemaInfo,
	selectedTables map[string][]string,
) ([]tableColumnTarget, int) {
	targets := make([]tableColumnTarget, 0)
	skippedTables := 0

	for _, schema := range schemas {
		tables, ok := selectedTables[schema.Name]
		if !ok || len(tables) == 0 {
			continue
		}
		sort.Strings(tables)

		for _, table := range tables {
			columnsCtx, cancel := context.WithTimeout(context.Background(), columnMetadataTimeout)
			columns, err := disc.GetColumns(columnsCtx, schema.Name, table)
			cancel()
			if err != nil {
				skippedTables++
				fmt.Printf("Skipping %s.%s: could not read columns: %v\n", schema.Name, table, err)
				continue
			}
			if len(columns) == 0 {
				skippedTables++
				fmt.Printf("Skipping %s.%s: no columns found.\n", schema.Name, table)
				continue
			}

			targets = append(targets, tableColumnTarget{
				Schema:  schema.Name,
				Table:   table,
				Columns: columns,
			})
		}
	}

	return targets, skippedTables
}

func estimateRemainingDuration(elapsed time.Duration, processedColumns, remainingColumns int) time.Duration {
	if processedColumns <= 0 || remainingColumns <= 0 {
		return 0
	}

	avgPerColumn := elapsed / time.Duration(processedColumns)
	if avgPerColumn <= 0 {
		return 0
	}

	return (avgPerColumn * time.Duration(remainingColumns)).Round(time.Second)
}

// selectDatabasesForTables handles the interactive database selection workflow.
func selectDatabasesForTables(cfg *config, dbCfg *databaseConfig, configPath string) ([]string, error) {
	defaultDB := strings.TrimSpace(dbCfg.Database)

	if defaultDB != "" {
		// Ask whether to use default database or select databases
		choice, err := promptSelectRequired(
			fmt.Sprintf("Database selection (default: %s)", defaultDB),
			[]string{
				fmt.Sprintf("Use default database (%s)", defaultDB),
				"Select databases",
			},
		)
		if err != nil {
			return nil, err
		}

		if strings.HasPrefix(choice, "Use default") {
			return []string{defaultDB}, nil
		}
	}

	// List available databases
	fmt.Println("Discovering available databases...")
	if dbCfg.Type == "snowflake" && dbCfg.Authenticator == "externalbrowser" {
		fmt.Println("Opening browser for SSO authentication...")
	}

	listerCfg := discovery.DatabaseConfig{
		Type:          dbCfg.Type,
		Database:      dbCfg.Database,
		Host:          dbCfg.Host,
		Port:          dbCfg.Port,
		User:          dbCfg.User,
		Password:      dbCfg.Password,
		SSLMode:       dbCfg.SSLMode,
		Account:       dbCfg.Account,
		Role:          dbCfg.Role,
		Warehouse:     dbCfg.Warehouse,
		Authenticator: dbCfg.Authenticator,
	}

	lister, err := discovery.NewDatabaseLister(listerCfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer lister.Close()

	timeout := 60 * time.Second
	if dbCfg.Authenticator == "externalbrowser" {
		timeout = 120 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	databases, err := lister.ListDatabases(ctx)
	if err != nil {
		return nil, fmt.Errorf("list databases: %w", err)
	}
	databases = normalizeDatabaseNames(databases)

	if len(databases) == 0 {
		return nil, fmt.Errorf("no databases discovered for connection %q", dbCfg.Name)
	}

	fmt.Printf("Found %d database(s)\n\n", len(databases))

	// Multi-select with "Select all" option
	selected, err := promptMultiSelectWithAll("Select databases", databases)
	if err != nil {
		return nil, err
	}

	if len(selected) == 0 {
		return nil, nil
	}

	// If no default database, prompt to save one
	if strings.TrimSpace(dbCfg.Database) == "" && len(selected) > 0 {
		updated, err := setConnectionDefaultDatabase(cfg, dbCfg.Name, selected[0])
		if err == nil && updated {
			if err := writeConfig(configPath, *cfg); err == nil {
				absConfigPath, _ := filepath.Abs(configPath)
				fmt.Printf("Saved default database %q to %s\n", selected[0], absConfigPath)
			}
		}
		dbCfg.Database = selected[0]
	}

	return selected, nil
}

// processDatabase handles schema selection and table detail discovery for one database.
func processDatabase(dbCfg databaseConfig, baseDir, database string) {
	discoveryCfg := toDiscoveryConfig(dbCfg)

	if dbCfg.Type == "snowflake" && dbCfg.Authenticator == "externalbrowser" {
		fmt.Println("Opening browser for SSO authentication...")
	}

	disc, err := discovery.NewTableDetailDiscoverer(discoveryCfg)
	if err != nil {
		fmt.Printf("Could not connect to database %q: %v\n", database, err)
		return
	}
	defer disc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Discover schemas
	schemas, err := discoverSchemasWithProgress(ctx, disc)
	if err != nil {
		fmt.Printf("Could not discover schemas for %q: %v\n", database, err)
		return
	}

	if len(schemas) == 0 {
		fmt.Println("No schemas found.")
		return
	}

	// Collect schema names in alphabetical order
	schemaNames := make([]string, len(schemas))
	for i, s := range schemas {
		schemaNames[i] = s.Name
	}
	sort.Strings(schemaNames)

	fmt.Printf("Found %d schema(s)\n\n", len(schemas))

	// Schema selection
	selectedSchemas, err := promptMultiSelectWithAll("Select schemas", schemaNames)
	if err != nil {
		fmt.Printf("Schema selection failed: %v\n", err)
		return
	}

	if len(selectedSchemas) == 0 {
		fmt.Println("No schemas selected.")
		return
	}

	// Build lookup for selected schemas
	selectedSet := make(map[string]bool, len(selectedSchemas))
	for _, s := range selectedSchemas {
		selectedSet[s] = true
	}

	// Generate context files — write each table immediately after discovery
	opts := contextgen.Options{
		ConnectionName: dbCfg.Name,
		DatabaseName:   database,
		DatabaseType:   dbCfg.Type,
		BaseDir:        baseDir,
	}

	// Count total tables across selected schemas for progress display
	totalTableCount := 0
	for _, schema := range schemas {
		if selectedSet[schema.Name] {
			totalTableCount += len(schema.Tables)
		}
	}

	if totalTableCount == 0 {
		fmt.Println("No tables to process.")
		return
	}

	tableIndex := 0

	for _, schema := range schemas {
		if !selectedSet[schema.Name] {
			continue
		}

		fmt.Printf("\nProcessing schema %q (%d tables)...\n", schema.Name, len(schema.Tables))

		for _, table := range schema.Tables {
			tableIndex++
			tableStart := time.Now()

			fmt.Printf("  [%d/%d] Processing %s.%s...\n", tableIndex, totalTableCount, schema.Name, table.Name)

			input := contextgen.TableDetailInput{
				Schema: schema.Name,
				Table:  table.Name,
			}

			// Get columns
			cols, err := disc.GetColumns(ctx, schema.Name, table.Name)
			if err != nil {
				fmt.Printf("    Skipping columns for %s.%s: %v\n", schema.Name, table.Name, err)
			} else {
				input.Columns = cols
			}

			// Get sample rows
			sample, err := disc.GetSampleRows(ctx, schema.Name, table.Name, 10)
			if err != nil {
				fmt.Printf("    Skipping sample for %s.%s: %v\n", schema.Name, table.Name, err)
			} else {
				input.Sample = sample
			}

			// Write files for this table immediately
			if err := contextgen.GenerateTableDetails([]contextgen.TableDetailInput{input}, opts); err != nil {
				fmt.Printf("    Error generating files for %s.%s: %v\n", schema.Name, table.Name, err)
				continue
			}

			elapsed := time.Since(tableStart).Round(time.Millisecond)
			if input.Columns != nil {
				fmt.Printf("    Wrote columns file for %s.%s\n", schema.Name, table.Name)
			}
			if input.Sample != nil && len(input.Sample.Rows) > 0 {
				fmt.Printf("    Wrote sample file for %s.%s\n", schema.Name, table.Name)
			}
			fmt.Printf("    Done %s.%s (%s)\n", schema.Name, table.Name, elapsed)
		}
	}

	fmt.Printf("\nProcessed %d table(s) across %d schema(s)\n", tableIndex, len(selectedSchemas))
}

func discoverSchemasWithProgress(ctx context.Context, disc discovery.Discoverer) ([]discovery.SchemaInfo, error) {
	fmt.Println("Discovering schemas...")

	startedAt := time.Now()
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				fmt.Printf("  Still discovering schemas... (%s elapsed)\n", time.Since(startedAt).Round(time.Second))
			}
		}
	}()

	schemas, err := disc.Discover(ctx)
	close(done)
	wg.Wait()

	elapsed := time.Since(startedAt).Round(time.Millisecond)
	if err != nil {
		fmt.Printf("Schema discovery failed after %s.\n", elapsed)
		return nil, err
	}

	fmt.Printf("Schema discovery completed in %s.\n", elapsed)
	return schemas, nil
}

// promptMultiSelectWithAll shows a multi-select prompt with a "Select all" option.
func promptMultiSelectWithAll(label string, options []string) ([]string, error) {
	if len(options) == 0 {
		return nil, fmt.Errorf("no options available")
	}

	choices := append([]string{"(Select all)"}, options...)

	opts := make([]huh.Option[string], len(choices))
	for i, o := range choices {
		opts[i] = huh.NewOption(o, o)
	}

	var selected []string
	if err := huh.NewMultiSelect[string]().
		Title(label).
		Description("Press <space> to toggle, <enter> to confirm").
		Options(opts...).
		Value(&selected).
		Run(); err != nil {
		return nil, err
	}

	// Check if "Select all" was chosen
	for _, s := range selected {
		if s == "(Select all)" {
			return options, nil
		}
	}

	return selected, nil
}

// toDiscoveryConfig converts a databaseConfig to a discovery.DatabaseConfig.
func toDiscoveryConfig(dbCfg databaseConfig) discovery.DatabaseConfig {
	return discovery.DatabaseConfig{
		Type:          dbCfg.Type,
		Database:      dbCfg.Database,
		Host:          dbCfg.Host,
		Port:          dbCfg.Port,
		User:          dbCfg.User,
		Password:      dbCfg.Password,
		SSLMode:       dbCfg.SSLMode,
		Account:       dbCfg.Account,
		Role:          dbCfg.Role,
		Warehouse:     dbCfg.Warehouse,
		Schema:        dbCfg.Schema,
		Authenticator: dbCfg.Authenticator,
	}
}

func ensureDefaultDatabaseForSchemas(cfg *config, dbCfg *databaseConfig, configPath string) error {
	if !requiresExplicitDatabaseSelection(dbCfg.Type) {
		return nil
	}
	if strings.TrimSpace(dbCfg.Database) != "" {
		return nil
	}

	fmt.Printf("No default database configured for connection %q.\n", dbCfg.Name)
	if dbCfg.Type == "snowflake" && dbCfg.Authenticator == "externalbrowser" {
		fmt.Println("Opening browser for SSO authentication...")
	}

	listerCfg := discovery.DatabaseConfig{
		Type:          dbCfg.Type,
		Database:      dbCfg.Database,
		Host:          dbCfg.Host,
		Port:          dbCfg.Port,
		User:          dbCfg.User,
		Password:      dbCfg.Password,
		SSLMode:       dbCfg.SSLMode,
		Account:       dbCfg.Account,
		Role:          dbCfg.Role,
		Warehouse:     dbCfg.Warehouse,
		Authenticator: dbCfg.Authenticator,
	}

	lister, err := discovery.NewDatabaseLister(listerCfg)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer lister.Close()

	timeout := 60 * time.Second
	if dbCfg.Authenticator == "externalbrowser" {
		timeout = 120 * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	databases, err := lister.ListDatabases(ctx)
	if err != nil {
		return fmt.Errorf("list databases: %w", err)
	}
	databases = normalizeDatabaseNames(databases)
	if len(databases) == 0 {
		return fmt.Errorf("no databases discovered for connection %q; configure a default database in .dbharness/config.json", dbCfg.Name)
	}

	selected, err := promptSelectRequired("Select a database for schema generation", databases)
	if err != nil {
		return fmt.Errorf("select default database: %w", err)
	}

	updated, err := setConnectionDefaultDatabase(cfg, dbCfg.Name, selected)
	if err != nil {
		return err
	}
	if updated {
		if err := writeConfig(configPath, *cfg); err != nil {
			return err
		}
		absConfigPath, _ := filepath.Abs(configPath)
		fmt.Printf("Saved default database %q to %s\n", selected, absConfigPath)
	}
	dbCfg.Database = selected
	fmt.Println()

	return nil
}

func runUpdateDatabases(args []string) {
	flags := flag.NewFlagSet("update-databases", flag.ExitOnError)
	shortName := flags.String("s", "", "Connection name from config.json.")
	longName := flags.String("name", "", "Connection name from config.json.")
	_ = flags.Parse(args)

	name := *shortName
	if name == "" {
		name = *longName
	}

	baseDir := filepath.Join(".", ".dbharness")
	configPath := filepath.Join(baseDir, "config.json")
	cfg, err := readConfig(configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	var dbCfg databaseConfig
	if name == "" {
		dbCfg, err = findPrimaryConnection(cfg)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	} else {
		dbCfg, err = findDatabaseConfig(cfg, name)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}

	fmt.Printf("Discovering databases for connection %q (%s)...\n", dbCfg.Name, dbCfg.Type)
	if dbCfg.Type == "snowflake" && dbCfg.Authenticator == "externalbrowser" {
		fmt.Println("Opening browser for SSO authentication...")
	}

	discoveryCfg := discovery.DatabaseConfig{
		Type:          dbCfg.Type,
		Host:          dbCfg.Host,
		Port:          dbCfg.Port,
		Database:      dbCfg.Database,
		User:          dbCfg.User,
		Password:      dbCfg.Password,
		SSLMode:       dbCfg.SSLMode,
		Account:       dbCfg.Account,
		Role:          dbCfg.Role,
		Warehouse:     dbCfg.Warehouse,
		Authenticator: dbCfg.Authenticator,
	}

	lister, err := discovery.NewDatabaseLister(discoveryCfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect: %v\n", err)
		os.Exit(1)
	}
	defer lister.Close()

	timeout := 60 * time.Second
	if dbCfg.Authenticator == "externalbrowser" {
		timeout = 120 * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	databases, err := lister.ListDatabases(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "list databases: %v\n", err)
		os.Exit(1)
	}
	databases = normalizeDatabaseNames(databases)

	fmt.Printf("Found %d database(s)\n", len(databases))
	for _, db := range databases {
		fmt.Printf("  %s\n", db)
	}
	fmt.Println()

	defaultDatabase := strings.TrimSpace(dbCfg.Database)
	if defaultDatabase == "" {
		switch len(databases) {
		case 0:
			defaultDatabase = "_default"
			fmt.Println("No default database is configured and no databases were discovered.")
			fmt.Println(`Using "_default" in _databases.yml for now.`)
		case 1:
			defaultDatabase = databases[0]
			fmt.Printf("No default database configured; using the only discovered database %q.\n", defaultDatabase)
		default:
			fmt.Printf("No default database configured for connection %q.\n", dbCfg.Name)
			defaultDatabase, err = promptSelectRequired("Select a default database", databases)
			if err != nil {
				fmt.Fprintf(os.Stderr, "select default database: %v\n", err)
				os.Exit(1)
			}
		}

		if defaultDatabase != "_default" {
			updated, err := setConnectionDefaultDatabase(&cfg, dbCfg.Name, defaultDatabase)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			if updated {
				if err := writeConfig(configPath, cfg); err != nil {
					fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				absConfigPath, _ := filepath.Abs(configPath)
				fmt.Printf("Saved default database %q to %s\n\n", defaultDatabase, absConfigPath)
			}
		}
	}
	dbCfg.Database = defaultDatabase

	opts := contextgen.Options{
		ConnectionName: dbCfg.Name,
		DatabaseName:   dbCfg.Database,
		DatabaseType:   dbCfg.Type,
		BaseDir:        baseDir,
	}

	added, err := contextgen.UpdateDatabasesFile(databases, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "update databases file: %v\n", err)
		os.Exit(1)
	}

	databasesDir := filepath.Join(baseDir, "context", "connections", dbCfg.Name, "databases")
	absPath, _ := filepath.Abs(filepath.Join(databasesDir, "_databases.yml"))

	if len(added) == 0 {
		fmt.Printf("No new databases found. %s is up to date.\n", absPath)
	} else {
		fmt.Printf("Added %d new database(s):\n", len(added))
		for _, name := range added {
			fmt.Printf("  + %s\n", name)
		}
		fmt.Printf("\nDatabases file written to %s\n", absPath)
	}
}

func printConnections(w io.Writer, cfg config) {
	if len(cfg.Connections) == 0 {
		fmt.Fprintln(w, "No connections configured.")
		return
	}

	fmt.Fprintln(w, "NAME\tTYPE\tHOST_URL")
	for _, entry := range cfg.Connections {
		hostURL := connectionHostURL(entry)
		if hostURL == "" {
			hostURL = "-"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\n", entry.Name, entry.Type, hostURL)
	}
}

func connectionHostURL(entry databaseConfig) string {
	host := strings.TrimSpace(entry.Host)
	if host != "" {
		if entry.Port > 0 {
			return fmt.Sprintf("%s:%d", host, entry.Port)
		}
		return host
	}

	if strings.EqualFold(strings.TrimSpace(entry.Type), "snowflake") {
		account := strings.TrimSpace(entry.Account)
		if account == "" {
			return ""
		}
		if strings.HasPrefix(account, "https://") || strings.HasPrefix(account, "http://") {
			return account
		}
		return fmt.Sprintf("https://%s.snowflakecomputing.com", account)
	}

	return ""
}

// findPrimaryConnection returns the connection marked as primary, or the
// first connection in the list if none is marked primary.
func findPrimaryConnection(cfg config) (databaseConfig, error) {
	if len(cfg.Connections) == 0 {
		return databaseConfig{}, fmt.Errorf("no connections configured in config.json")
	}
	for _, c := range cfg.Connections {
		if c.Primary {
			return c, nil
		}
	}
	return cfg.Connections[0], nil
}

// sanitizeSchemaName normalises a schema name for use as a directory name.
func sanitizeSchemaName(name string) string {
	r := strings.NewReplacer("/", "_", "\\", "_", " ", "_", ".", "_")
	return strings.ToLower(r.Replace(name))
}

func requiresExplicitDatabaseSelection(databaseType string) bool {
	switch strings.ToLower(strings.TrimSpace(databaseType)) {
	case "postgres", "snowflake":
		return true
	default:
		return false
	}
}

func normalizeDatabaseNames(names []string) []string {
	seen := make(map[string]bool, len(names))
	normalized := make([]string, 0, len(names))
	for _, raw := range names {
		name := strings.TrimSpace(raw)
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true
		normalized = append(normalized, name)
	}
	sort.Strings(normalized)
	return normalized
}

func setConnectionDefaultDatabase(cfg *config, connectionName, database string) (bool, error) {
	database = strings.TrimSpace(database)
	if database == "" {
		return false, fmt.Errorf("default database cannot be empty")
	}

	for i := range cfg.Connections {
		if cfg.Connections[i].Name != connectionName {
			continue
		}
		if strings.TrimSpace(cfg.Connections[i].Database) == database {
			return false, nil
		}
		cfg.Connections[i].Database = database
		return true, nil
	}

	return false, fmt.Errorf("connection %q not found in config", connectionName)
}

func setPrimaryConnection(cfg *config, connectionName string) (string, bool, error) {
	if cfg == nil {
		return "", false, fmt.Errorf("config cannot be nil")
	}

	connectionName = strings.TrimSpace(connectionName)
	if connectionName == "" {
		return "", false, fmt.Errorf("connection name cannot be empty")
	}

	selectedIndex := -1
	previousPrimary := ""
	for i := range cfg.Connections {
		if cfg.Connections[i].Name == connectionName {
			selectedIndex = i
		}
		if cfg.Connections[i].Primary && previousPrimary == "" {
			previousPrimary = cfg.Connections[i].Name
		}
	}

	if selectedIndex == -1 {
		return "", false, fmt.Errorf("connection %q not found in config", connectionName)
	}

	changed := false
	for i := range cfg.Connections {
		shouldBePrimary := i == selectedIndex
		if cfg.Connections[i].Primary != shouldBePrimary {
			cfg.Connections[i].Primary = shouldBePrimary
			changed = true
		}
	}

	return previousPrimary, changed, nil
}

func ensureGitignore() {
	const entry = ".dbharness-snapshots/"
	gitignorePath := filepath.Join(".", ".gitignore")

	data, err := os.ReadFile(gitignorePath)
	if err != nil {
		os.WriteFile(gitignorePath, []byte(entry+"\n"), 0o644)
		return
	}

	for _, line := range strings.Split(string(data), "\n") {
		if strings.TrimSpace(line) == entry {
			return
		}
	}

	f, err := os.OpenFile(gitignorePath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer f.Close()

	f.WriteString("\n" + entry + "\n")
}

func readConfig(path string) (config, error) {
	file, err := os.Open(path)
	if err != nil {
		return config{}, fmt.Errorf("open config: %w", err)
	}
	defer file.Close()

	var cfg config
	if err := json.NewDecoder(file).Decode(&cfg); err != nil {
		return config{}, fmt.Errorf("decode config: %w", err)
	}
	return cfg, nil
}

func findDatabaseConfig(cfg config, name string) (databaseConfig, error) {
	for _, entry := range cfg.Connections {
		if entry.Name == name {
			return entry, nil
		}
	}

	return databaseConfig{}, fmt.Errorf("database %q not found in config", name)
}

func pingDatabase(entry databaseConfig) error {
	switch entry.Type {
	case "postgres":
		return pingPostgres(entry)
	case "snowflake":
		return pingSnowflake(entry)
	default:
		return fmt.Errorf("unsupported database type %q", entry.Type)
	}
}

func pingPostgres(entry databaseConfig) error {
	if entry.SSLMode == "" {
		entry.SSLMode = "disable"
	}

	connString := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		entry.Host,
		entry.Port,
		entry.User,
		entry.Password,
		entry.Database,
		entry.SSLMode,
	)

	db, err := sql.Open("postgres", connString)
	if err != nil {
		return fmt.Errorf("open postgres connection: %w", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}

	return nil
}

func pingSnowflake(entry databaseConfig) error {
	sfConfig := &gosnowflake.Config{
		Account:   entry.Account,
		User:      entry.User,
		Password:  entry.Password,
		Role:      entry.Role,
		Warehouse: entry.Warehouse,
		Database:  entry.Database,
		Schema:    entry.Schema,
	}

	switch entry.Authenticator {
	case "externalbrowser":
		sfConfig.Authenticator = gosnowflake.AuthTypeExternalBrowser
	default:
		sfConfig.Authenticator = gosnowflake.AuthTypeSnowflake
	}

	dsn, err := gosnowflake.DSN(sfConfig)
	if err != nil {
		return fmt.Errorf("build snowflake DSN: %w", err)
	}

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		return fmt.Errorf("open snowflake connection: %w", err)
	}
	defer db.Close()

	timeout := 10 * time.Second
	if entry.Authenticator == "externalbrowser" {
		timeout = 120 * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping snowflake: %w", err)
	}

	return nil
}

func installTemplate(targetDir string, force bool) (string, error) {
	var snapshotPath string

	if info, err := os.Stat(targetDir); err == nil {
		if !info.IsDir() {
			return "", fmt.Errorf("target exists and is not a directory: %s", targetDir)
		}
		if !force {
			return "", fmt.Errorf("target already exists: %s (use --force to overwrite)", targetDir)
		}

		ensureGitignore()
		snapshotPath, err = snapshotDirectory(targetDir)
		if err != nil {
			return "", fmt.Errorf("snapshot existing .dbharness: %w", err)
		}
		if err := os.RemoveAll(targetDir); err != nil {
			return "", fmt.Errorf("remove existing target: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("check target: %w", err)
	}

	root, err := template.Root()
	if err != nil {
		return "", fmt.Errorf("load template: %w", err)
	}

	if err := copyFS(root, targetDir); err != nil {
		return "", err
	}

	return snapshotPath, nil
}

func createSnapshotDir(sourceDir string) (string, error) {
	timestamp := time.Now().Format("20060102_1504_05")
	snapshotDir := filepath.Join(filepath.Dir(sourceDir), ".dbharness-snapshots", timestamp)
	if err := os.MkdirAll(snapshotDir, 0o755); err != nil {
		return "", err
	}
	return snapshotDir, nil
}

func snapshotDirectory(sourceDir string) (string, error) {
	snapshotDir, err := createSnapshotDir(sourceDir)
	if err != nil {
		return "", err
	}

	source := os.DirFS(sourceDir)
	if err := copyFS(source, snapshotDir); err != nil {
		return "", err
	}

	return snapshotDir, nil
}

func copyFS(source fs.FS, targetDir string) error {
	return fs.WalkDir(source, ".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		destPath := filepath.Join(targetDir, path)
		if entry.IsDir() {
			return os.MkdirAll(destPath, 0o755)
		}

		data, err := fs.ReadFile(source, path)
		if err != nil {
			return err
		}

		return os.WriteFile(destPath, data, 0o644)
	})
}

var stdinReader = bufio.NewReader(os.Stdin)

func readLine() string {
	line, _ := stdinReader.ReadString('\n')
	return strings.TrimSpace(line)
}

func promptYesNo(label string) bool {
	fmt.Printf("%s (y/n): ", label)
	answer := strings.ToLower(readLine())
	return answer == "y" || answer == "yes"
}

func promptString(label, defaultVal string) string {
	fmt.Printf("%s (%s): ", label, defaultVal)
	input := readLine()
	if input == "" {
		return defaultVal
	}
	return input
}

func promptStringRequired(label string) string {
	for {
		fmt.Printf("%s: ", label)
		input := readLine()
		if input != "" {
			return input
		}
		fmt.Println("  Value is required.")
	}
}

func promptInt(label string, defaultVal int) int {
	for {
		fmt.Printf("%s: ", label)
		input := readLine()
		if input == "" {
			return defaultVal
		}
		val, err := strconv.Atoi(input)
		if err != nil {
			fmt.Println("  Please enter a valid number.")
			continue
		}
		return val
	}
}

func promptSelect(label string, options []string) string {
	opts := make([]huh.Option[string], len(options))
	for i, o := range options {
		opts[i] = huh.NewOption(o, o)
	}
	var result string
	huh.NewSelect[string]().
		Title(label).
		Options(opts...).
		Value(&result).
		Run()
	return result
}

func promptSelectRequired(label string, options []string) (string, error) {
	if len(options) == 0 {
		return "", fmt.Errorf("no options available to select")
	}

	opts := make([]huh.Option[string], len(options))
	for i, o := range options {
		opts[i] = huh.NewOption(o, o)
	}

	var result string
	if err := huh.NewSelect[string]().
		Title(label).
		Options(opts...).
		Value(&result).
		Run(); err != nil {
		return "", err
	}

	result = strings.TrimSpace(result)
	if result == "" {
		return "", fmt.Errorf("no option selected")
	}
	return result, nil
}

func writeConfig(path string, cfg config) error {
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}

func collectPostgresConfig(entry *databaseConfig) {
	entry.Host = promptStringRequired("Host")
	entry.Port = promptInt("Port (press Enter for 5432)", 5432)
	entry.Database = promptStringRequired("Database")
	entry.User = promptStringRequired("User")
	entry.Password = promptStringRequired("Password")
	entry.SSLMode = promptSelect("SSL Mode", []string{"require", "disable"})
}

func collectSnowflakeConfig(entry *databaseConfig) {
	entry.Account = promptStringRequired("Account (e.g. org-account_name)")
	auth := promptSelect("Authenticator", []string{"externalbrowser", "snowflake username & password"})
	if auth == "snowflake username & password" {
		entry.Authenticator = "snowflake"
	} else {
		entry.Authenticator = auth
	}
	entry.User = promptStringRequired("User")
	if entry.Authenticator == "snowflake" {
		entry.Password = promptStringRequired("Password")
	}
	entry.Role = promptStringRequired("Role")
	entry.Warehouse = promptStringRequired("Warehouse")
	fmt.Print("Default database (optional, press Enter to skip): ")
	entry.Database = readLine()
	fmt.Print("Default schema (optional, press Enter to skip): ")
	entry.Schema = readLine()
}

func addConnectionEntry(targetDir string, firstInit bool) {
	configPath := filepath.Join(targetDir, "config.json")
	cfg, err := readConfig(configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	var name string
	for {
		name = promptStringRequired("Connection name")
		if _, err := findDatabaseConfig(cfg, name); err != nil {
			break
		}
		fmt.Printf("  %q already exists, choose another.\n", name)
	}

	dbType := promptSelect("Database type", []string{"postgres", "snowflake"})
	environment := promptSelect("Environment", []string{
		"production", "staging", "development", "local", "testing", "(skip for now)",
	})
	if environment == "(skip for now)" {
		environment = ""
	}

	entry := databaseConfig{
		Name:        name,
		Environment: environment,
		Type:        dbType,
	}

	switch dbType {
	case "postgres":
		collectPostgresConfig(&entry)
	case "snowflake":
		collectSnowflakeConfig(&entry)
	}

	primary := firstInit
	if !firstInit {
		fmt.Println()
		fmt.Println("The primary connection is used by default when running commands")
		fmt.Println("like test-connection without specifying a connection name.")
		primary = promptYesNo("Set as primary connection?")
	}
	entry.Primary = primary

	fmt.Println()
	fmt.Printf("Testing connection to %s...\n", name)
	if entry.Type == "snowflake" && entry.Authenticator == "externalbrowser" {
		fmt.Println("Opening browser for SSO authentication...")
	}
	if err := pingDatabase(entry); err != nil {
		fmt.Fprintf(os.Stderr, "Connection failed: %v\n", err)
		fmt.Fprintln(os.Stderr, "\nDatabase config was not saved. Please check your connection details and try again.")
		os.Exit(1)
	}
	fmt.Println("Connection ok!")
	fmt.Println()

	if primary {
		for i := range cfg.Connections {
			cfg.Connections[i].Primary = false
		}
	}

	cfg.Connections = append(cfg.Connections, entry)
	if err := writeConfig(configPath, cfg); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	absPath, _ := filepath.Abs(configPath)
	fmt.Printf("Added %q to %s\n", name, absPath)
}
