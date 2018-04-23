package Common

import (
	"database/sql"
	"helpers"
	log "github.com/sirupsen/logrus"
	"fmt"
)

type Common struct {
	TableDesc helpers.Table
	Statistics helpers.TableStats
}

func (t *Common) GetTableDesc() helpers.Table {
	log.Debug(fmt.Sprintf("Exposing table information"))
	return t.TableDesc
}

func (t *Common) GetTableStats() helpers.TableStats {
	log.Debug(fmt.Sprintf("Exposing table stats"))
	return t.Statistics
}

func (t *Common) SetDeleteQuery(Values string, config *helpers.Config) {
	helpers.SetDeleteQuery(&t.TableDesc, Values, config)
}

func (t *Common) SetSelectQuery(config *helpers.Config) {
	helpers.SetSelectQuery(&t.TableDesc, config)
}

func (t *Common) Run(config *helpers.Config) {
	helpers.Run(&t.TableDesc, &t.Statistics, config)
}

func (t *Common) Prepare(connection *sql.DB, config *helpers.Config) {
	helpers.Prepare(&t.TableDesc, &t.Statistics, connection, config)
}

func (t *Common) RowsIterator(rows *sql.Rows) (string, int) {
	return helpers.RowsIterator(&t.Statistics, rows)
}
