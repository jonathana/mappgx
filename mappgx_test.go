package mappgx

import (
	"fmt"
	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
	satori "github.com/jackc/pgx/pgtype/ext/satori-uuid"
	shopspring "github.com/jackc/pgx/pgtype/ext/shopspring-numeric"
	"github.com/satori/go.uuid"
	"github.com/shopspring/decimal"
	"net"
	"reflect"
	"testing"
	"time"
)


func getConn() (*pgx.Conn, error){
	const connString = "pgx://mappgx_user:foobar@localhost/mappgx_test"
	connCfg, err := pgx.ParseConnectionString(connString)
	if err != nil {
		return nil, fmt.Errorf("unable to parse connection string %s", connString)
	}
	conn, err := pgx.Connect(connCfg)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database %s on %s", connCfg.Database, connCfg.Host)
	}

	conn.ConnInfo.RegisterDataType(pgtype.DataType{
		Value: &satori.UUID{},
		Name:  "uuid",
		OID:   pgtype.UUIDOID,
	})
	conn.ConnInfo.RegisterDataType(pgtype.DataType{
		Value: &shopspring.Numeric{},
		Name:  "uuid",
		OID:   pgtype.NumericOID,
	})

	return conn, nil
}

func TestNonExistentQuery(t *testing.T) {
	conn, err := getConn()
	if err != nil {
		t.Error(err)
	}
	_, err = RowsMap(conn, "SELECT * FROM asdfjqwreasdfpuqetrnafdh")
	if err != nil {
		pgErr, ok := err.(pgx.PgError)
		if !ok {
			t.Error(err)
		}
		if pgErr.Severity != "ERROR" && pgErr.Code == "42P01" {
			t.Error(err)
		}
	}
}

func TestBasicQueryNoRows(t *testing.T) {
	conn, err := getConn()
	if err != nil {
		t.Error(err)
	}

	rm, err := RowsMap(conn, "SELECT * FROM mappgx_test_table WHERE 1 = 0")

	if err != nil {
		t.Error(err)
	}

	if len(rm) != 0 {
		t.Error(err)
	}
}

func TestSingleErrorsOnNoRows(t *testing.T) {
	conn, err := getConn()
	if err != nil {
		t.Error(err)
	}

	rm, err := RowMap(conn, "SELECT * FROM mappgx_test_table WHERE 1 = 0")

	if err != nil {
		if err != pgx.ErrNoRows {
			t.Error(err)
		}
	} else {
		t.Error(fmt.Errorf("expected to get error for no rows but call succeeded"))
	}

	if len(rm) != 0 {
		t.Error(err)
	}
}

func TestBasicQuery(t *testing.T) {
	conn, err := getConn()
	if err != nil {
		t.Error(err)
	}

	rms, err := RowsMap(conn, "SELECT * FROM mappgx_test_table LIMIT 1")

	if err != nil {
		t.Error(err)
	}

	if len(rms) != 1 {
		t.Error(err)
	}

	rm := rms[0]
	validateBasicRow(rm, t)
}

func TestBasicSingleQuery(t *testing.T) {
	conn, err := getConn()
	if err != nil {
		t.Error(err)
	}

	rm, err := RowMap(conn, "SELECT * FROM mappgx_test_table LIMIT 1")

	if err != nil {
		t.Error(err)
	}

	validateBasicRow(rm, t)
}

func validateBasicRow(rm map[string]interface{}, t *testing.T) {
	/*
	typeMap := map[string]reflect.Type {
		"date_added": time.Time,
		"test_cidr": "*net.IPNet",
		"id": "int32",
		"test_int": "int64",
		"test_text": "string",
		"test_uuid": "uuid.UUID",
		"test_decimal": "decimal.Decimal",
	}
	*/
	for key, value := range(rm) {
		switch value.(type) {
		case int32:
			if key != "test_int" {
				t.Error(fmt.Errorf("field %s was a %s but expected int32", key, reflect.TypeOf(value).Name()))
			}
			break
		case int64:
			if key != "id" {
				t.Error(fmt.Errorf("field %s was a %s but expected int32", key, reflect.TypeOf(value).Name()))
			}
			break
		case string:
			if key != "test_text" {
				t.Error(fmt.Errorf("field %s was a %s but expected string", key, reflect.TypeOf(value).Name()))
			}
			break
		case uuid.UUID:
			if key != "test_uuid" {
				t.Error(fmt.Errorf("field %s was a %s but expected uuid.UUID", key, reflect.TypeOf(value).Name()))
			}
			break
		case decimal.Decimal:
			if key != "test_decimal" {
				t.Error(fmt.Errorf("field %s was a %s but expected uuid.UUID", key, reflect.TypeOf(value).Name()))
			}
			break
		case time.Time:
			if key != "date_added" {
				t.Error(fmt.Errorf("field %s was a %s but expected uuid.UUID", key, reflect.TypeOf(value).Name()))
			}
			break
		case *net.IPNet:
			if key != "test_cidr" {
				t.Error(fmt.Errorf("field %s was a %s but expected uuid.UUID", key, reflect.TypeOf(value).Name()))
			}
			break
		case map[string]interface{}:
			if key != "test_jsonb" {
				t.Error(fmt.Errorf("field %s was a %s but expected uuid.UUID", key, reflect.TypeOf(value).Name()))
			}
			break
		default:
			t.Error(fmt.Errorf("field %s has unknown type %s", key, reflect.TypeOf(value).Name()))
		}
		/*
		if reflect.TypeOf(value).Name() != typeMap[key] {
			t.Error(fmt.Errorf("Field %s was of type %s but expected %s", key, reflect.TypeOf(value), typeMap[key]))
		}
		*/
	}
}

