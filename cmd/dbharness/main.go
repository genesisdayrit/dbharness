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

	"github.com/genesisdayrit/dbharness/internal/template"
	_ "github.com/lib/pq"
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
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "Usage:")
	fmt.Fprintln(os.Stderr, "  dbharness init [--force]")
	fmt.Fprintln(os.Stderr, "  dbharness test-connection [-s name]")
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
		if promptYesNo("Would you like to add a new database config?") {
			addDatabaseEntry(targetDir)
		} else {
			fmt.Println("No changes made.")
		}
		return
	}

	if err := installTemplate(targetDir, *force); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	fmt.Printf("Installed .dbharness to %s\n", targetDir)
}

type config struct {
	Connections []databaseConfig `json:"connections"`
}

type databaseConfig struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Database string `json:"database"`
	User     string `json:"user"`
	Password string `json:"password"`
	SSLMode  string `json:"sslmode"`
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
	if len(cfg.Connections) == 0 {
		return config{}, fmt.Errorf("config has no connections")
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
	default:
		return fmt.Errorf("unsupported database type %q (only postgres is supported)", entry.Type)
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
		fmt.Printf("%s (%d): ", label, defaultVal)
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

func promptOption(label string, options []string, defaultVal string) string {
	optionList := strings.Join(options, ", ")
	for {
		fmt.Printf("%s [%s]\n  (%s): ", label, optionList, defaultVal)
		input := readLine()
		if input == "" {
			return defaultVal
		}
		for _, opt := range options {
			if input == opt {
				return input
			}
		}
		fmt.Printf("  Invalid option. Choose from: %s\n", optionList)
	}
}

func writeConfig(path string, cfg config) error {
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}

func addDatabaseEntry(targetDir string) {
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

	dbType := promptString("Database type", "postgres")
	host := promptStringRequired("Host")
	port := promptInt("Port", 5432)
	database := promptStringRequired("Database")
	user := promptStringRequired("User")
	password := promptStringRequired("Password")
	sslMode := promptOption("SSL Mode", []string{"require", "disable"}, "require")

	entry := databaseConfig{
		Name:     name,
		Type:     dbType,
		Host:     host,
		Port:     port,
		Database: database,
		User:     user,
		Password: password,
		SSLMode:  sslMode,
	}

	fmt.Println()
	fmt.Printf("Testing connection to %s...\n", name)
	if err := pingDatabase(entry); err != nil {
		fmt.Fprintf(os.Stderr, "Connection failed: %v\n", err)
		fmt.Fprintln(os.Stderr, "\nDatabase config was not saved. Please check your connection details and try again.")
		os.Exit(1)
	}
	fmt.Println("Connection ok!")
	fmt.Println()

	cfg.Connections = append(cfg.Connections, entry)
	if err := writeConfig(configPath, cfg); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	absPath, _ := filepath.Abs(configPath)
	fmt.Printf("Added %q to %s\n", name, absPath)
}
