package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/charmbracelet/huh"
	"github.com/genesisdayrit/dbharness/internal/contextgen"
	"github.com/genesisdayrit/dbharness/internal/discovery"
	"github.com/genesisdayrit/dbharness/internal/template"
	_ "github.com/lib/pq"
	"github.com/snowflakedb/gosnowflake"
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
	case "schemas":
		runSchemas(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "Usage:")
	fmt.Fprintln(os.Stderr, "  dbharness init [--force]")
	fmt.Fprintln(os.Stderr, "  dbharness test-connection [-s name]")
	fmt.Fprintln(os.Stderr, "  dbharness snapshot")
	fmt.Fprintln(os.Stderr, "  dbharness snapshot config")
	fmt.Fprintln(os.Stderr, "  dbharness schemas [-s name]")
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

	if err := installTemplate(targetDir, *force); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
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

	timestamp := time.Now().Format("20060102_1504_05")
	snapshotDir := filepath.Join(".", ".dbharness-snapshots", timestamp)
	sourceDir := filepath.Join(".", ".dbharness")

	if configOnly {
		srcPath := filepath.Join(sourceDir, "config.json")
		data, err := os.ReadFile(srcPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "read config: %v\n", err)
			os.Exit(1)
		}
		if err := os.MkdirAll(snapshotDir, 0o755); err != nil {
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
		source := os.DirFS(sourceDir)
		if err := os.MkdirAll(snapshotDir, 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "create snapshot dir: %v\n", err)
			os.Exit(1)
		}
		if err := copyFS(source, snapshotDir); err != nil {
			fmt.Fprintf(os.Stderr, "snapshot: %v\n", err)
			os.Exit(1)
		}
		absPath, _ := filepath.Abs(snapshotDir)
		fmt.Printf("Snapshot saved to %s\n", absPath)
	}
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
	cfg, err := readConfig(filepath.Join(baseDir, "config.json"))
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

func installTemplate(targetDir string, force bool) error {
	if info, err := os.Stat(targetDir); err == nil {
		if !info.IsDir() {
			return fmt.Errorf("target exists and is not a directory: %s", targetDir)
		}
		if !force {
			return fmt.Errorf("target already exists: %s (use --force to overwrite)", targetDir)
		}
		if err := os.RemoveAll(targetDir); err != nil {
			return fmt.Errorf("remove existing target: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("check target: %w", err)
	}

	root, err := template.Root()
	if err != nil {
		return fmt.Errorf("load template: %w", err)
	}

	return copyFS(root, targetDir)
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
