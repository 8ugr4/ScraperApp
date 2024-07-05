package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"log"
	"sync"
)

const (
	//inputTableCreateSQLQuery   = `CREATE TABLE IF NOT EXISTS InputTable(url String)engine = MergeTree()order by url`
	selectColumnSQLQuery      = "select distinct url from InputTable"
	dropTableSQLQuery         = "DROP TABLE IF EXISTS OutputTable"
	outputTableSQLQuery       = "CREATE TABLE IF NOT EXISTS OutputTable(url String, status String,body_length Int)engine = MergeTree()order by status"
	insertOutputTableSQLQuery = "INSERT INTO OutputTable (url,status,body_length) VALUES (?, ?, ?) "
)

type chTransfer struct {
	*URL
}

// Counts the rows of the table, could definitely be improved using another built-in function from driver.Conn.

func (ct *chTransfer) CountRows(conn driver.Conn) int {

	ctx := context.Background()

	rows, err := conn.Query(ctx, fmt.Sprintf("%s", selectColumnSQLQuery))

	if err != nil {
		fmt.Printf("couldn't select from table: %q", err)
	}

	defer func(rows driver.Rows) {
		err := rows.Close()
		if err != nil {

		}
	}(rows)
	i := 0

	for rows.Next() {
		i++
		var url string
		if err := rows.Scan(&url); err != nil {
			log.Printf("couldn't scan the rows :%v \n", err)
		}
	}

	return i
}

// reads the rows from the table. sends them to be parsed.

func (ct *chTransfer) readFromCh(conn driver.Conn, inChan chan string) error {

	ctx := context.Background()

	rows, err := conn.Query(ctx, fmt.Sprintf("%s", selectColumnSQLQuery))
	if err != nil {
		return fmt.Errorf("couldn't select from table: %q", err)
	}
	defer func(rows driver.Rows) {
		err := rows.Close()
		if err != nil {

		}
	}(rows)

	for rows.Next() {

		var url string
		if err := rows.Scan(&url); err != nil {
			log.Fatalf("couldn't scan the rows :%v \n", err)
		}
		inChan <- url
	}
	return nil
}

// reads the parsed URL's as struct, INSERT's into OutputTable in Ch.

func (ct *chTransfer) writeIntoCh(wg *sync.WaitGroup, conn driver.Conn, ch1 chan *Response, count int) error {

	ctx := context.Background()
	defer wg.Done()

	if err := conn.Exec(ctx, fmt.Sprintf("%s", dropTableSQLQuery)); err != nil {
		fmt.Printf("%q\n", err)
	}

	err := conn.Exec(ctx, fmt.Sprintf("%s", outputTableSQLQuery))
	if err != nil {
		return fmt.Errorf("couldn't create table: %q \n", err)
	}

	err1 := conn.Exec(ctx, "INSERT into OutputTable (url,status,body_length) values ('www.testTEST-test.com','1234 okOK',1230321)")
	if err1 != nil {
		return fmt.Errorf("couldn't insert into table %q\n", err1)
	}

	for i := 0; i < count; i++ {

		Parsed := <-ch1

		err := conn.Exec(ctx, insertOutputTableSQLQuery, Parsed.Url, Parsed.Status, Parsed.Length)
		if err != nil {
			return fmt.Errorf("\ncould not append to the Table,\nERROR: : %q\n", err)
		}

	}

	return nil
}

func (u *URL) connect() (driver.Conn, error) {

	ctx := context.Background()

	host, port := u.chHost, u.chPort

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", host, port)},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Debug: true,
		Debugf: func(format string, v ...interface{}) {
			//log.Printf("debug : "+format, v...)
		},
	})
	if err != nil {
		return nil, fmt.Errorf("couldn't open clickhouse: %v", err)
	}

	err1 := conn.Ping(ctx)
	if err1 != nil {
		var exception *clickhouse.Exception
		if errors.As(err1, &exception) {
			fmt.Printf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return nil, fmt.Errorf("couldn't ping clickhouse: %v", err)
	}
	return conn, nil
}
