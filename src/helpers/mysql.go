package helpers

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"time"
	"fmt"
	log "github.com/sirupsen/logrus"
)

const(
	driverName = "mysql"
	informationSchema = "information_schema"
	informationTable = "TABLES"
	dataLength = "DATA_LENGTH"
	dataFree = "DATA_FREE"
	sqlConnectionString = "{USERNAME}:{PASSWORD}@tcp({HOST}:3306)/{DATABASE}" +
		"?charset=utf8&tls=true&maxAllowedPacket=67108864&tls=skip-verify"
	fragmentationQuery = "SELECT {DATA_LENGTH},{DATA_FREE} FROM {ISCHEMA}.{ITABLE}" +
		" WHERE TABLE_NAME='{TABLE}'"
)

func AtomicQuery(connection *sql.DB, query string, status *bool) {
	log.Debug(fmt.Sprintf("Begin transaction"))
	transaction, err := connection.Begin()
	LogErrorVar(err, status)
	log.Debug(fmt.Sprintf("Running query: %s", query))
	_, e := transaction.Query(query)
	if e != nil {
		log.Debug(fmt.Sprintf("Transaction failed! Performing rollback!"))
		transaction.Rollback()
		LogErrorVar(e, status)
	} else {
		log.Debug(fmt.Sprintf("Transaction commit"))
		transaction.Commit()
	}
	log.Debug(fmt.Sprintf("End transaction"))
}

func SimpleQuery(connection *sql.DB, query string, status *bool) (*sql.Rows) {
	log.Debug(fmt.Sprintf("Running query: %s", query))
	rows, err := connection.Query(query)
	LogErrorVar(err, status)
	return rows
}

func SelectQuery(connection *sql.DB, query string, status *bool) (*sql.Rows) {
	log.Debug(fmt.Sprintf("Running SELECT"))
	return SimpleQuery(connection, query, status)
}

func DeleteQuery(connection *sql.DB, query string, status *bool) {
	log.Debug(fmt.Sprintf("Running DELETE"))
	AtomicQuery(connection, query, status)
}

func StatusQuery(connection *sql.DB, table string, status *bool) (*sql.Rows) {
	query := FormatString(
		fragmentationQuery,
		"{DATA_LENGTH}", dataLength,
		"{DATA_FREE}", dataFree,
		"{ISCHEMA}", informationSchema,
		"{ITABLE}", informationTable,
		"{TABLE}", table,
	)
	log.Debug(fmt.Sprintf("Running status query: %s", query))
	return SelectQuery(connection, query, status)
}

func IsFragmentationRequired(connection *sql.DB, table string, threshold int, status *bool) bool {
	var (
		tablespaceSize, tablespaceFree  float64
	)
	result := StatusQuery(connection, table, status)
	result.Next()
	result.Scan(
		&tablespaceSize,
		&tablespaceFree,
	)
	if CountPercent(tablespaceFree, tablespaceSize) > float64(threshold) {
		log.Debug(fmt.Sprintf("Fragmentation required for table: %s!", table))
		return true
	}
	log.Debug(fmt.Sprintf("Fragmentation is not required for table: %s!", table))
	return false
}

func OpenConnection(username string, password string, host string, database string) (*sql.DB, error) {
	connectionString := FormatString(
		sqlConnectionString,
		"{USERNAME}", username,
		"{PASSWORD}", password,
		"{HOST}", host,
		"{DATABASE}", database,
	)
	log.Debug(fmt.Sprintf("Initiating DB connection with: %s", connectionString))
	connection, err := sql.Open(driverName, connectionString)
	connection.SetConnMaxLifetime(30 * time.Minute)
	connection.SetMaxIdleConns(4)
	connection.SetMaxOpenConns(10)
	log.Debug(fmt.Sprintf("Setting connection properties"))
	return connection, err
}
