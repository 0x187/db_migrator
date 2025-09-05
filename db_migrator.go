package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"runtime"
	"strings"
	"sync"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

// CONFIGURATION
// Holds the database connection strings.
type Config struct {
	SQLServerConnectionString string `json:"sqlserver_connection_string"`
	MySQLConnectionString     string `json:"mysql_connection_string"`
}

// ColumnSchema defines the structure of a table column from the source DB.
type ColumnSchema struct {
	Name             string
	Type             string
	IsNullable       string
	CharMaxLength    sql.NullInt64
	NumericPrecision sql.NullInt64
	NumericScale     sql.NullInt64
}

// PrimaryKeyInfo holds information about a table's primary key.
type PrimaryKeyInfo struct {
	ColumnName string
	IsNumeric  bool
}

// NEW: A threshold to decide if a table is "large" and should be chunked.
const largeTableThreshold int64 = 100000 // e.g., 100,000 rows

// main is the entry point of the application.
func main() {
	// --- Configuration Loading ---
	configFile, err := os.Open("config.json")
	if err != nil {
		log.Fatalf("FATAL: Could not open config.json. Error: %v", err)
	}
	defer configFile.Close()

	byteValue, _ := ioutil.ReadAll(configFile)
	var config Config
	json.Unmarshal(byteValue, &config)

	if config.SQLServerConnectionString == "" || config.MySQLConnectionString == "" {
		log.Fatal("FATAL: Connection strings in config.json cannot be empty.")
	}

	// --- Database Connections ---
	sqlserverDB, err := sql.Open("sqlserver", config.SQLServerConnectionString)
	if err != nil {
		log.Fatalf("FATAL: Failed to create SQL Server connection pool. Error: %v", err)
	}
	defer sqlserverDB.Close()

	mysqlDB, err := sql.Open("mysql", config.MySQLConnectionString)
	if err != nil {
		log.Fatalf("FATAL: Failed to create MySQL connection pool. Error: %v", err)
	}
	defer mysqlDB.Close()

	// Set connection pool limits for better performance
	sqlserverDB.SetMaxOpenConns(runtime.NumCPU() * 2)
	mysqlDB.SetMaxOpenConns(runtime.NumCPU() * 2)

	log.Println("Pinging databases...")
	if err := sqlserverDB.Ping(); err != nil {
		log.Fatalf("FATAL: Cannot ping SQL Server. Check connection string and network. Error: %v", err)
	}
	if err := mysqlDB.Ping(); err != nil {
		log.Fatalf("FATAL: Cannot ping MySQL. Check connection string and network. Error: %v", err)
	}
	log.Println("SUCCESS: Both databases are connected.")

	// --- Migration Process ---
	tables, err := getTables(sqlserverDB)
	if err != nil {
		log.Fatalf("FATAL: Could not fetch table list from source. Error: %v", err)
	}
	if len(tables) == 0 {
		log.Println("No user tables found to migrate. Exiting.")
		return
	}

	log.Printf("Found %d tables to migrate. Starting process...", len(tables))

	var wg sync.WaitGroup
	// NEW: Professional progress bar container
	p := mpb.New(mpb.WithWaitGroup(&wg), mpb.WithWidth(60))

	// NEW: Channel to collect errors from workers
	errorChan := make(chan string, len(tables))

	// Determine the number of concurrent table migrations
	maxWorkers := runtime.NumCPU()
	if len(tables) < maxWorkers {
		maxWorkers = len(tables)
	}
	tableChan := make(chan string, len(tables))
	for _, table := range tables {
		tableChan <- table
	}
	close(tableChan)

	// Start worker pool for processing tables
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for tableName := range tableChan {
				migrateTable(sqlserverDB, mysqlDB, tableName, p, errorChan)
			}
		}()
	}

	p.Wait()

	// After all workers are done, check for errors
	close(errorChan)
	var migrationErrors []string
	for errStr := range errorChan {
		migrationErrors = append(migrationErrors, errStr)
	}

	fmt.Println("\n--- Migration Summary ---")
	if len(migrationErrors) > 0 {
		fmt.Printf("❌ Migration finished with %d errors:\n", len(migrationErrors))
		for _, errMsg := range migrationErrors {
			fmt.Printf("- %s\n", errMsg)
		}
	} else {
		fmt.Println("✅ Migration completed successfully for all tables!")
	}
}

// migrateTable handles the migration of a single table, deciding if it should be chunked.
func migrateTable(sqlserverDB, mysqlDB *sql.DB, tableName string, p *mpb.Progress, errorChan chan<- string) {
	// 1. Get row count to determine strategy
	var rowCount int64
	err := sqlserverDB.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM [%s] WITH (NOLOCK)", tableName)).Scan(&rowCount)
	if err != nil {
		errorChan <- fmt.Sprintf("Table [%s]: Failed to get row count: %v", tableName, err)
		return
	}

	// 2. Get table schema and PK info
	columns, err := getTableSchema(sqlserverDB, tableName)
	if err != nil {
		errorChan <- fmt.Sprintf("Table [%s]: Failed to get schema: %v", tableName, err)
		return
	}
	pkInfo, err := getPrimaryKey(sqlserverDB, tableName)
	if err != nil {
		errorChan <- fmt.Sprintf("Table [%s]: Failed to get primary key: %v", tableName, err)
		return
	}

	// 3. Create table in MySQL
	createStatement := generateCreateTableStatement(tableName, columns, pkInfo)
	if _, err := mysqlDB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", tableName)); err != nil {
		errorChan <- fmt.Sprintf("Table [%s]: Failed to drop existing table in MySQL: %v", tableName, err)
		return
	}
	if _, err := mysqlDB.Exec(createStatement); err != nil {
		errorChan <- fmt.Sprintf("Table [%s]: Failed to create table in MySQL: %v\nSQL: %s", tableName, err, createStatement)
		return
	}

	// 4. Decide migration strategy: chunked or single-threaded
	if rowCount > largeTableThreshold && pkInfo.IsNumeric {
		// Use the advanced chunking method for large tables
		err = migrateLargeTableInChunks(sqlserverDB, mysqlDB, tableName, rowCount, pkInfo, p, errorChan)
	} else {
		// Use the standard method for smaller tables or tables without numeric PK
		err = migrateSmallTable(sqlserverDB, mysqlDB, tableName, rowCount, p, errorChan)
	}

	if err != nil {
		// Specific errors are already sent to the channel inside the functions
		// This is a fallback
		errorChan <- fmt.Sprintf("Table [%s]: Migration failed with error: %v", tableName, err)
	}
}

// migrateSmallTable copies data for a small table in a single thread.
func migrateSmallTable(sourceDB, destDB *sql.DB, tableName string, totalRows int64, p *mpb.Progress, errorChan chan<- string) error {
	bar := p.AddBar(totalRows,
		mpb.PrependDecorators(
			decor.Name(fmt.Sprintf("%-25.25s", tableName), decor.WC{W: 25, C: decor.DidentRight}),
			decor.Counters(0, "%d / %d", decor.WC{W: 15, C: decor.DidentRight}),
		),
		mpb.AppendDecorators(
			decor.Percentage(decor.WC{W: 5}),
			decor.NewAverageETA(decor.ET_STYLE_GO, decor.WC{W: 8}),
		),
	)

	rows, err := sourceDB.QueryContext(context.Background(), fmt.Sprintf("SELECT * FROM [%s] WITH (NOLOCK)", tableName))
	if err != nil {
		bar.Abort(false)
		return err
	}
	defer rows.Close()

	return copyData(destDB, tableName, rows, bar)
}

// NEW: migrateLargeTableInChunks parallelizes the migration of one large table.
func migrateLargeTableInChunks(sourceDB, destDB *sql.DB, tableName string, totalRows int64, pkInfo PrimaryKeyInfo, p *mpb.Progress, errorChan chan<- string) error {
	// 1. Get min and max of the primary key to create chunks
	var minPK, maxPK int64
	pkQuery := fmt.Sprintf("SELECT MIN([%s]), MAX([%s]) FROM [%s] WITH (NOLOCK)", pkInfo.ColumnName, pkInfo.ColumnName, tableName)
	err := sourceDB.QueryRow(pkQuery).Scan(&minPK, &maxPK)
	if err != nil || (minPK == 0 && maxPK == 0) {
		// Fallback to small table migration if we can't get a valid range
		return migrateSmallTable(sourceDB, destDB, tableName, totalRows, p, errorChan)
	}

	bar := p.AddBar(totalRows,
		mpb.PrependDecorators(
			decor.Name(fmt.Sprintf("%-25.25s", tableName), decor.WC{W: 25, C: decor.DidentRight}),
			decor.Counters(0, "%d / %d", decor.WC{W: 15, C: decor.DidentRight}),
		),
		mpb.AppendDecorators(
			decor.Percentage(decor.WC{W: 5}),
			decor.Name("chunking", decor.WC{W: 8, C: decor.Dcolor}),
		),
	)

	// 2. Calculate chunk size and number of workers
	numWorkers := runtime.NumCPU()
	rangeSize := maxPK - minPK + 1
	chunkSize := int64(math.Ceil(float64(rangeSize) / float64(numWorkers)))
	if chunkSize < 1 {
		chunkSize = 1
	}

	var chunkWg sync.WaitGroup
	chunkErrorChan := make(chan error, numWorkers)

	for i := 0; i < numWorkers; i++ {
		startPK := minPK + (int64(i) * chunkSize)
		endPK := startPK + chunkSize - 1

		if startPK > maxPK {
			continue // No more data to process
		}

		chunkWg.Add(1)
		go func(start, end int64) {
			defer chunkWg.Done()

			// Each goroutine needs its own DB connection from the pool
			conn, err := sourceDB.Conn(context.Background())
			if err != nil {
				chunkErrorChan <- err
				return
			}
			defer conn.Close()

			query := fmt.Sprintf("SELECT * FROM [%s] WITH (NOLOCK) WHERE [%s] BETWEEN %d AND %d", tableName, pkInfo.ColumnName, start, end)
			rows, err := conn.QueryContext(context.Background(), query)
			if err != nil {
				chunkErrorChan <- err
				return
			}
			defer rows.Close()

			err = copyData(destDB, tableName, rows, bar)
			if err != nil {
				chunkErrorChan <- err
			}
		}(startPK, endPK)
	}

	chunkWg.Wait()
	close(chunkErrorChan)

	// Check if any chunk had an error
	for err := range chunkErrorChan {
		bar.Abort(false)
		return err // Return the first error encountered
	}

	bar.SetTotal(bar.Current(), true) // Mark as complete
	return nil
}

// --- Database Utility Functions ---

// getTables retrieves a list of all user-defined tables from the SQL Server database.
func getTables(db *sql.DB) ([]string, error) {
	rows, err := db.Query("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = 'dbo'")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	return tables, nil
}

// getTableSchema retrieves the schema for a specific table.
func getTableSchema(db *sql.DB, tableName string) ([]ColumnSchema, error) {
	query := `
		SELECT 
			COLUMN_NAME, 
			DATA_TYPE, 
			IS_NULLABLE, 
			CHARACTER_MAXIMUM_LENGTH,
			NUMERIC_PRECISION,
			NUMERIC_SCALE
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_NAME = @p1
		ORDER BY ORDINAL_POSITION
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

// getPrimaryKey finds the primary key for a given table.
func getPrimaryKey(db *sql.DB, tableName string) (PrimaryKeyInfo, error) {
	query := `
		SELECT 
			kcu.COLUMN_NAME,
			col.DATA_TYPE
		FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
		JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu 
			ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
			AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
			AND tc.TABLE_NAME = kcu.TABLE_NAME
		JOIN INFORMATION_SCHEMA.COLUMNS AS col
			ON kcu.COLUMN_NAME = col.COLUMN_NAME
			AND kcu.TABLE_SCHEMA = col.TABLE_SCHEMA
			AND kcu.TABLE_NAME = col.TABLE_NAME
		WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' AND tc.TABLE_NAME = @p1 AND tc.TABLE_SCHEMA = 'dbo'
	`
	var pkColName, dataType string
	err := db.QueryRow(query, tableName).Scan(&pkColName, &dataType)
	if err != nil {
		if err == sql.ErrNoRows {
			return PrimaryKeyInfo{}, nil // No primary key found, not an error
		}
		return PrimaryKeyInfo{}, err
	}

	isNumeric := false
	switch strings.ToLower(dataType) {
	case "int", "smallint", "tinyint", "bigint":
		isNumeric = true
	}

	return PrimaryKeyInfo{ColumnName: pkColName, IsNumeric: isNumeric}, nil
}

// copyData performs the actual data transfer from a result set to the destination table.
func copyData(destDB *sql.DB, tableName string, rows *sql.Rows, bar *mpb.Bar) error {
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	if len(columns) == 0 {
		return nil // No columns in result set
	}

	// Use a transaction for batch inserts
	tx, err := destDB.Begin()
	if err != nil {
		return err
	}
	// Defer a rollback. If the commit is successful, the rollback is a no-op.
	defer tx.Rollback()

	columnPlaceholders := strings.Repeat("?,", len(columns))
	columnPlaceholders = columnPlaceholders[:len(columnPlaceholders)-1]

	insertQuery := fmt.Sprintf(
		"INSERT INTO `%s` (`%s`) VALUES (%s)",
		tableName,
		strings.Join(columns, "`,`"),
		columnPlaceholders,
	)

	stmt, err := tx.Prepare(insertQuery)
	if err != nil {
		return err
	}
	defer stmt.Close()

	// Prepare for scanning rows
	values := make([]interface{}, len(columns))
	scanArgs := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	batchSize := 200 // Number of rows to insert in a single batch
	batchCount := 0

	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return err
		}

		if _, err := stmt.Exec(values...); err != nil {
			return err
		}
		batchCount++
	}

	if err = rows.Err(); err != nil {
		return err
	}

	// Commit the transaction
	if err = tx.Commit(); err != nil {
		return err
	}

	bar.IncrBy(batchCount)
	return nil
}

// --- Statement Generation Functions ---

// generateCreateTableStatement creates a MySQL-compatible CREATE TABLE statement.
func generateCreateTableStatement(tableName string, columns []ColumnSchema, pkInfo PrimaryKeyInfo) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE TABLE `%s` (\n", tableName))

	for i, col := range columns {
		colDef := fmt.Sprintf("  `%s` %s %s",
			col.Name,
			convertSQLServerTypeToMySQL(col),
			ternary(col.IsNullable == "YES", "NULL", "NOT NULL"),
		)
		sb.WriteString(colDef)
		if i < len(columns)-1 {
			sb.WriteString(",\n")
		}
	}

	if pkInfo.ColumnName != "" {
		sb.WriteString(",\n")
		sb.WriteString(fmt.Sprintf("  PRIMARY KEY (`%s`)", pkInfo.ColumnName))
	}

	sb.WriteString("\n) ENGINE=InnoDB CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;")
	return sb.String()
}

// convertSQLServerTypeToMySQL maps SQL Server data types to their MySQL equivalents.
func convertSQLServerTypeToMySQL(col ColumnSchema) string {
	switch strings.ToLower(col.Type) {
	case "nvarchar", "varchar", "char", "nchar", "text", "ntext":
		if col.CharMaxLength.Valid && col.CharMaxLength.Int64 == -1 {
			return "LONGTEXT"
		}
		if col.CharMaxLength.Valid && col.CharMaxLength.Int64 > 0 {
			// Cap length to MySQL's max for VARCHAR
			if col.CharMaxLength.Int64 > 16383 {
				return "MEDIUMTEXT"
			}
			return fmt.Sprintf("VARCHAR(%d)", col.CharMaxLength.Int64)
		}
		return "TEXT"
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
	case "binary", "varbinary":
		if col.CharMaxLength.Valid && col.CharMaxLength.Int64 == -1 {
			return "LONGBLOB"
		}
		if col.CharMaxLength.Valid && col.CharMaxLength.Int64 > 0 {
			if col.CharMaxLength.Int64 > 65535 {
				return "MEDIUMBLOB"
			}
			return fmt.Sprintf("VARBINARY(%d)", col.CharMaxLength.Int64)
		}
		return "BLOB"
	case "image":
		return "LONGBLOB"
	default:
		return "TEXT"
	}
}

// ternary is a simple utility for inline conditional statements.
func ternary(condition bool, trueVal, falseVal string) string {
	if condition {
		return trueVal
	}
	return falseVal
}
