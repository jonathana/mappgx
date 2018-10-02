package mappgx

import (
	"fmt"
	"github.com/jackc/pgx"
)

var queryFieldMapCache = map[string][]string{}

func ColumnMap(fds []pgx.FieldDescription) []string {
	//var colNames [len(fds)]string
	colNames := make([]string, len(fds), len(fds))

	for _, fd := range fds {
		colNames[fd.AttributeNumber - 1] = fd.Name
	}
	return colNames
}

func retrieveColumnMapForQuery(sql string, fds []pgx.FieldDescription) []string {
	colMap, ok := queryFieldMapCache[sql]
	if !ok {
		colMap = ColumnMap(fds)
		queryFieldMapCache[sql] = colMap
	}

	return colMap
}

func RowsMap(p *pgx.Conn, sql string, args ...interface{}) ([]map[string]interface{}, error) {
	rs, err := p.Query(sql, args...)
	if err != nil {
		return []map[string]interface{}{}, err
	}
	cmq := retrieveColumnMapForQuery(sql, rs.FieldDescriptions())
	var rms []map[string]interface{}
	for rs.Next() {
		vals, err := rs.Values()
		if err != nil {
			return []map[string]interface{}{}, err
		}
		if len(cmq) != len(vals) {
			return []map[string]interface{}{}, fmt.Errorf("query had %d column definitions but row returned %d values", len(cmq), len(vals))
		}

		rm := map[string]interface{}{}
		for i, val := range vals {
			rm[cmq[i]] = val
		}
		rms = append(rms, rm)
	}

	return rms, nil
}

func RowMap(p *pgx.Conn, sql string, args ...interface{}) (map[string]interface{}, error) {
	rs, err := p.Query(sql, args...)
	if err != nil {
		return map[string]interface{}{}, err
	}
	cmq := retrieveColumnMapForQuery(sql, rs.FieldDescriptions())
	rm := map[string]interface{}{}
	hadRow := false
	for rs.Next() {
		vals, err := rs.Values()
		if err != nil {
			return map[string]interface{}{}, err
		}

		if hadRow {
			return map[string]interface{}{}, fmt.Errorf("query returned more than 1 row")
		} else {
			hadRow = true
		}

		if len(cmq) != len(vals) {
			return map[string]interface{}{}, fmt.Errorf("query had %d column definitions but row returned %d values", len(cmq), len(vals))
		}

		rm := map[string]interface{}{}
		for i, val := range vals {
			rm[cmq[i]] = val
		}
	}

	if !hadRow {
		return map[string]interface{}{}, pgx.ErrNoRows
	}

	return rm, nil
}
