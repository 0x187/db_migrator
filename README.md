Go Database Migration Tool: SQL Server to MySQL
A high-performance Go script for concurrent data migration from SQL Server to MySQL.

🚀 Quick Start
Configure config.json: Create this file in the project root with your database credentials.

{
  "sqlserver_connection_string": "sqlserver://user:password@host:port?database=SourceDB",
  "mysql_connection_string": "user:password@tcp(host:port)/DestinationDB?parseTime=true"
}

Note: The destination database must already exist.

Install Dependencies:

go get [github.com/denisenkom/go-mssqldb](https://github.com/denisenkom/go-mssqldb)
go get [github.com/go-sql-driver/mysql](https://github.com/go-sql-driver/mysql)

Run Migration:

go run db_migrator.go

⚠️ Warning
This script will delete existing tables in the destination database (DROP TABLE IF EXISTS). Always back up your destination database before running.
