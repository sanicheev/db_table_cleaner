package helpers

import (
	"fmt"
	"time"
	"database/sql"
	log "github.com/sirupsen/logrus"
)

const(
	delimiter = ","
)

func SetDeleteQuery(TableDesc *Table, Values string, config *Config) {
	TableDesc.DeleteQuery = FormatString(
		config.DeleteQuery,
		"{VALUE}", Values,
	)
	log.Debug(fmt.Sprintf("Set delete query to: %s", TableDesc.DeleteQuery))
}

func SetSelectQuery(TableDesc *Table, config *Config) {
	TableDesc.SelectQuery = FormatString(
		config.SelectQuery,
		"{THRESHOLD}", IntToString(TableDesc.SelectThreshold),
		"{LIMIT}", IntToString(TableDesc.ResultLimit),
	)
	log.Debug(fmt.Sprintf("Set select query to: %s", TableDesc.SelectQuery))
}

func Run(TableDesc *Table, TableStats *TableStats, config *Config) {
	log.Debug(fmt.Sprintf("Running in run stage"))
	empty := false
	for empty == false {
		data, rowsFetched := RowsIterator(
			TableStats,
			SelectQuery(
				TableDesc.Connection,
				TableDesc.SelectQuery,
				&TableStats.ExitStatus,
			),
		)
		if rowsFetched > 0 {
			log.Debug(fmt.Sprintf(
				"Result set is not empty. Contains: %d entries. Performing cleanup", rowsFetched,
			))
			SetDeleteQuery(
				TableDesc,
				data,
				config,
			)
			DeleteQuery(
				TableDesc.Connection,
				TableDesc.DeleteQuery,
				&TableStats.ExitStatus,
			)
			TableStats.RowsProcessed += rowsFetched
		} else {
			log.Debug(fmt.Sprintf("Result set is empty. Nothing to do"))
			empty = true
		}
	}
	log.Debug(fmt.Sprintf("Checking if fragmentation required"))
	TableStats.FragmentationRequired = IsFragmentationRequired(
		TableDesc.Connection,
		TableDesc.TableName,
		TableDesc.SelectThreshold,
		&TableStats.ExitStatus,
	)
	TableStats.EndTime = time.Now().UTC()
	log.Debug(fmt.Sprintf("Setting execution end time to: %v", TableStats.EndTime))
	TableStats.TimeTaken = TableStats.EndTime.Sub(TableStats.StartTime)
	log.Debug(fmt.Sprintf("Time taken to perform clean: %v", TableStats.TimeTaken))
}

func Prepare(TableDesc *Table, TableStats *TableStats, connection *sql.DB, config *Config) {
	log.Debug(fmt.Sprintf("Running in prepare stage"))
	log.Debug(fmt.Sprintf("Configuring table descriptor"))
	*TableDesc = Table{
		TableName: config.TableName,
		Connection: connection,
		SelectThreshold: config.SelectThreshold,
		ResultLimit: config.ResultLimit,
	}
	TableStats.StartTime = time.Now().UTC()
	log.Debug(fmt.Sprintf("Setting execution start time to: %v", TableStats.StartTime))
	SetSelectQuery(TableDesc, config)
}

func RowsIterator(TableStats *TableStats, rows *sql.Rows) (string, int) {
	var (
		data []int
		rowsFetched int
	)
	for rows.Next() {
		var ApiLogId int
		err := rows.Scan(&ApiLogId)
		LogErrorVar(err, &TableStats.ExitStatus)
		data = append(data, ApiLogId)
		rowsFetched++
	}
	log.Debug(fmt.Sprintf("Processed rows: %d and obtained data: %v", rowsFetched, data))
	defer rows.Close()
	return ArrayToString(data, delimiter), rowsFetched
}
