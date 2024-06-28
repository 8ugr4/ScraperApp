package main

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"log"
	"reflect"
)

type chTransfer struct {
	*URL
	*Response
}

func (r *Response) readFromClick(inChan chan string) {

	conn, err := connect()
	if err != nil { log.Fatalf("err",err) }
	ctx := context.Background()

	dataRows, err := conn.Query(ctx, "select url from u.chInputTablename")
	if err != nil { log.Fatalf("err: %v", err) }

	defer func(dataRows driver.Rows) {
		err := dataRows.Close()
		if err != nil { log.Fatalf("err: %v", err) }
	}(dataRows)

	if hasRows := dataRows.Next(); hasRows {
		inChan <- dataRows
		for dataRows.Next() {
			inChan <- dataRows.Columns()


			inChan <- Response{Url: url1,}
			}

		}
	}

} else { log.Println("No more rows.") }
}

func (r *Response) writeIntoDatabase(ch1 chan *Response) {
	conn, err := connect()
	if err != nil {
		panic((err))
	}

	ctx := context.Background()

	//rows, err := conn.Query(ctx, "CHECK_TABLE OutputTable")

	err = conn.Exec(ctx,
		"CREATE TABLE IF NOT EXISTS OutputTable ("+
			"url String,"+
			"status String,"+
			"body_length Int"+
			"engine = MergeTree()order by id;")

		if err != nil { log.Fatalf("error : %v \n", err) }

	for range ch1 {
		urlParsed := <-ch1

		query, err1 := conn.Query(ctx, "insert into url_table("+
			"url,status,length)", urlParsed)

		if err1 != nil { log.Fatalf(" query:%v\n error:%v\n", query, err1) }
	}
}

func connect() (driver.Conn, error) {
	ctx := context.Background()

	//the difference between two files is var defining but why?

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"localhost:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Debug: true,
		Debugf: func(format string, v ...interface{}) { // will be executed if Debug is true
			//log.Printf("debug : "+format, v...)
		},
	})

	if err != nil {
		return nil, err
	}

	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return nil, err
	}
	return conn, nil
}
