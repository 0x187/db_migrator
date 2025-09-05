package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	// SQL Server driver
	_ "github.com/denisenkom/go-mssqldb"
	// MySQL driver
	_ "github.com/go-sql-driver/mysql"
)

// Config struct holds the database connection strings read from the JSON file.
type Config struct {
	SQLServerConnectionString string `json:"sqlserver_connection_string"`
	MySQLConnectionString     string `json:"mysql_connection_string"`
}

// ColumnSchema holds the structure of a database column.
type ColumnSchema struct {
	Name             string
	Type             string
	IsNullable       string
	CharMaxLength    sql.NullInt64
	NumericPrecision sql.NullInt64
	NumericScale     sql.NullInt64
}

// BATCH_SIZE defines how many rows are inserted in a single bulk INSERT statement.
const BATCH_SIZE = 500

func main() {
	// 0. Load configuration from config.json
	config, err := loadConfig("config.json")
	if err != nil {
		log.Fatalf("Error loading configuration: %v", err)
	}
	fmt.Println("⚙️ Configuration loaded successfully from config.json.")

	// 1. Connect to databases
	sourceDB, err := connectToDB("sqlserver", config.SQLServerConnectionString, "SQL Server")
	if err != nil {
		log.Fatal(err)
	}
	defer sourceDB.Close()

	destDB, err := connectToDB("mysql", config.MySQLConnectionString, "MySQL")
	if err != nil {
		log.Fatal(err)
	}
	defer destDB.Close()

	// 2. Get the list of tables
	tables, err := getSourceTables(sourceDB)
	if err != nil {
		log.Fatalf("Error getting table list: %v", err)
	}
	if len(tables) == 0 {
		fmt.Println("⚠️ No tables found in the source database.")
		return
	}
	fmt.Printf("🔍 Found %d tables to migrate. Starting process...\n", len(tables))

	// 3. Start the migration process
	var successfulTables, failedTables []string
	for _, tableName := range tables {
		fmt.Printf("\n--------------------------------------------------\n")
		fmt.Printf(" MIGRATING TABLE: %s\n", tableName)
		fmt.Printf("--------------------------------------------------\n")

		err := migrateTable(sourceDB, destDB, tableName)
		if err != nil {
			log.Printf("❌ [ERROR] Migration failed for table '%s': %v", tableName, err)
			failedTables = append(failedTables, tableName)
		} else {
			fmt.Printf("✅ [SUCCESS] Table '%s' migrated successfully.\n", tableName)
			successfulTables = append(successfulTables, tableName)
		}
	}

	// 4. Display migration summary
	fmt.Println("\n================= MIGRATION SUMMARY =================")
	fmt.Printf("Total tables processed: %d\n", len(tables))
	fmt.Printf("✅ Successful: %d\n", len(successfulTables))
	fmt.Printf("❌ Failed: %d\n", len(failedTables))
	if len(failedTables) > 0 {
		fmt.Printf("Failed tables: %s\n", strings.Join(failedTables, ", "))
	}
	fmt.Println("=====================================================")
}

// migrateTable manages the entire migration process for a single table.
func migrateTable(sourceDB, destDB *sql.DB, tableName string) error {
	// Step 1: Get table schema
	schema, err := getTableSchema(sourceDB, tableName)
	if err != nil {
		return fmt.Errorf("could not get table schema: %w", err)
	}
	fmt.Println("  [1/4] ✔️ Table schema fetched.")

	// Step 2: Get primary key
	primaryKeys, err := getPrimaryKey(sourceDB, tableName)
	if err != nil {
		return fmt.Errorf("could not get primary key: %w", err)
	}
	fmt.Println("  [2/4] ✔️ Primary key fetched.")

	// Step 3: Create table in the destination
	createStatement := generateCreateTableSQL(tableName, schema, primaryKeys)
	if _, err := destDB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`;", tableName)); err != nil {
		return fmt.Errorf("could not drop existing table: %w", err)
	}
	if _, err := destDB.Exec(createStatement); err != nil {
		return fmt.Errorf("could not create table: %w", err)
	}
	fmt.Println("  [3/4] ✔️ Table created in destination.")

	// Step 4: Copy data
	err = copyTableData(sourceDB, destDB, tableName)
	if err != nil {
		return fmt.Errorf("could not copy table data: %w", err)
	}
	fmt.Println("  [4/4] ✔️ Table data copied.")

	return nil
}

// connectToDB is a helper function to connect and ping a database.
func connectToDB(driver, dsn, dbName string) (*sql.DB, error) {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, fmt.Errorf("error preparing connection to %s: %w", dbName, err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("could not connect to %s: %w", dbName, err)
	}
	fmt.Printf("✅ Successfully connected to %s.\n", dbName)
	return db, nil
}

// loadConfig loads configuration from a JSON file.
func loadConfig(filename string) (*Config, error) {
	file, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("configuration file '%s' not found: %w", filename, err)
	}

	var config Config
	err = json.Unmarshal(file, &config)
	if err != nil {
		return nil, fmt.Errorf("error parsing configuration file: %w", err)
	}
	return &config, nil
}

// getSourceTables returns a list of all user tables from the SQL Server database.
func getSourceTables(db *sql.DB) ([]string, error) {
	query := "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME;"
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables = append(tables, name)
	}
	return tables, nil
}

// getTableSchema reads the schema of a specific table from SQL Server.
func getTableSchema(db *sql.DB, tableName string) ([]ColumnSchema, error) {
	query := `
		SELECT 
			COLUMN_NAME, DATA_TYPE, IS_NULLABLE, 
			CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_NAME = @p1
		ORDER BY ORDINAL_POSITION;
	`
	rows, err := db.Query(query, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []ColumnSchema
	for rows.Next() {
		var col ColumnSchema
		if err := rows.Scan(&col.Name, &col.Type, &col.IsNullable, &col.CharMaxLength, &col.NumericPrecision, &col.NumericScale); err != nil {
			return nil, err
		}
		columns = append(columns, col)
	}
	return columns, nil
}

// getPrimaryKey returns the primary key columns for a given table.
func getPrimaryKey(db *sql.DB, tableName string) ([]string, error) {
	query := `
		SELECT k.COLUMN_NAME
		FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS c
		JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS k
		ON c.CONSTRAINT_NAME = k.CONSTRAINT_NAME
		WHERE c.TABLE_NAME = @p1 AND c.CONSTRAINT_TYPE = 'PRIMARY KEY'
		ORDER BY k.ORDINAL_POSITION;
	`
	rows, err := db.Query(query, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var pkColumns []string
	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			return nil, err
		}
		pkColumns = append(pkColumns, colName)
	}
	return pkColumns, nil
}

// generateCreateTableSQL generates the `CREATE TABLE` query for MySQL.
func generateCreateTableSQL(tableName string, schema []ColumnSchema, primaryKeys []string) string {
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("CREATE TABLE `%s` (\n", tableName))

	for i, col := range schema {
		columnDef := fmt.Sprintf("  `%s` %s %s", col.Name, convertSQLServerTypeToMySQL(col), Ternary(col.IsNullable == "YES", "NULL", "NOT NULL"))
		builder.WriteString(columnDef)
		if i < len(schema)-1 || len(primaryKeys) > 0 {
			builder.WriteString(",\n")
		}
	}

	if len(primaryKeys) > 0 {
		pkCols := strings.Join(primaryKeys, "`, `")
		builder.WriteString(fmt.Sprintf("  PRIMARY KEY (`%s`)", pkCols))
	}

	builder.WriteString("\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;")
	return builder.String()
}

// convertSQLServerTypeToMySQL converts a SQL Server data type to its MySQL equivalent.
func convertSQLServerTypeToMySQL(col ColumnSchema) string {
	switch strings.ToLower(col.Type) {
	case "nvarchar", "varchar", "char", "nchar":
		// Handle (n)varchar(max) which has a max length of -1 in SQL Server's metadata
		if col.CharMaxLength.Valid && col.CharMaxLength.Int64 == -1 {
			return "LONGTEXT"
		}
		if col.CharMaxLength.Valid && col.CharMaxLength.Int64 > 0 && col.CharMaxLength.Int64 <= 16383 {
			return fmt.Sprintf("VARCHAR(%d)", col.CharMaxLength.Int64)
		}
		return "TEXT"
	case "text", "ntext":
		return "LONGTEXT"
	case "int":
		return "INT"
	case "smallint":
		return "SMALLINT"
	case "tinyint":
		return "TINYINT"
	case "bigint":
		return "BIGINT"
	case "bit":
		return "TINYINT(1)"
	case "decimal", "numeric":
		if col.NumericPrecision.Valid && col.NumericScale.Valid {
			return fmt.Sprintf("DECIMAL(%d, %d)", col.NumericPrecision.Int64, col.NumericScale.Int64)
		}
		return "DECIMAL(18, 2)"
	case "money", "smallmoney":
		return "DECIMAL(19, 4)"
	case "float":
		return "DOUBLE"
	case "real":
		return "FLOAT"
	case "datetime", "smalldatetime", "datetime2":
		return "DATETIME"
	case "date":
		return "DATE"
	case "time":
		return "TIME"
	case "uniqueidentifier":
		return "CHAR(36)"
	case "binary", "varbinary", "image":
		return "BLOB"
	default:
		return "TEXT"
	}
}

// copyTableData copies data efficiently using batch inserts.
func copyTableData(sourceDB, destDB *sql.DB, tableName string) error {
	rows, err := sourceDB.Query(fmt.Sprintf("SELECT * FROM [%s];", tableName))
	if err != nil {
		return err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	if len(columns) == 0 {
		return nil
	}

	valueArgs := make([]interface{}, len(columns))
	scanArgs := make([]interface{}, len(columns))
	for i := range valueArgs {
		scanArgs[i] = &valueArgs[i]
	}

	tx, err := destDB.Begin()
	if err != nil {
		return err
	}

	totalRowsCopied := 0
	batch := make([][]interface{}, 0, BATCH_SIZE)

	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			tx.Rollback()
			return err
		}

		rowData := make([]interface{}, len(columns))
		copy(rowData, valueArgs)
		batch = append(batch, rowData)

		if len(batch) == BATCH_SIZE {
			if err := insertBatch(tx, tableName, columns, batch); err != nil {
				tx.Rollback()
				return err
			}
			totalRowsCopied += len(batch)
			batch = batch[:0] // Reset batch
		}
	}

	// Insert any remaining rows
	if len(batch) > 0 {
		if err := insertBatch(tx, tableName, columns, batch); err != nil {
			tx.Rollback()
			return err
		}
		totalRowsCopied += len(batch)
	}

	if err := rows.Err(); err != nil {
		tx.Rollback()
		return err
	}

	fmt.Printf("    -> %d records copied.", totalRowsCopied)
	return tx.Commit()
}

// insertBatch constructs and executes a multi-row INSERT statement.
func insertBatch(tx *sql.Tx, tableName string, columns []string, batch [][]interface{}) error {
	if len(batch) == 0 {
		return nil
	}

	valueStrings := make([]string, 0, len(batch))
	valueArgs := make([]interface{}, 0, len(batch)*len(columns))

	for _, rowData := range batch {
		valueStrings = append(valueStrings, "(?"+strings.Repeat(",?", len(columns)-1)+")")
		valueArgs = append(valueArgs, rowData...)
	}

	stmt := fmt.Sprintf("INSERT INTO `%s` (`%s`) VALUES %s", tableName, strings.Join(columns, "`,`"), strings.Join(valueStrings, ","))

	_, err := tx.Exec(stmt, valueArgs...)
	return err
}

// Ternary is a simple helper for better readability, mimicking a ternary operator.
func Ternary[T any](condition bool, trueVal, falseVal T) T {
	if condition {
		return trueVal
	}
	return falseVal
}
