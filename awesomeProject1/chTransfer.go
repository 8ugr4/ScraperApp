package main

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"log"
)

type chTransfer struct {
	*URL
}

func (ct *chTransfer) DebugInCh() {

	conn, err := connect()
	if err != nil {
		log.Fatal("connection error :%v \n", err)
	}

	ctx := context.Background()

	err = conn.Exec(ctx,
		"CREATE TABLE IF NOT EXISTS InputTable(url String)engine = MergeTree()order by status;")
	if err != nil {
		log.Fatalf("error12 : %v \n", err)
	}
	fmt.Println("End of Debug")

	dataRows, err := conn.Query(ctx, "select url from InputTable")
	if err != nil {
		log.Fatalf("err13: %v", err)
	}
	for dataRows.Next() {
		var url string
		if err := dataRows.Scan(&url); err != nil {
			log.Fatalf("err %v \n", err)
		}
		fmt.Println(url)
	}

	defer func(dataRows driver.Rows) {
		err := dataRows.Close()
		if err != nil {
			log.Fatalf("err14: %v", err)
		}
	}(dataRows)

	//batch, err := conn.PrepareBatch(ctx, "INSERT INTO InputTable(url) values ('https://www.benocs.com/careers/'),"+
	//	"('https://drstearns.github.io/tutorials/gojson/'),;")
	//if err != nil {
	//	log.Fatalf("error7 : %v \n", err)
	//}

}

func (ct *chTransfer) readFromClick(inChan chan string) error {

	//ct.DebugCH()

	conn, err := connect()
	if err != nil {
		log.Fatalf("err1 %v", err)
	}
	ctx := context.Background()

	dataRows, err := conn.Query(ctx, "select url from InputTable")
	if err != nil {
		log.Fatalf("err2: %v", err)
	}

	defer func(dataRows driver.Rows) {
		err := dataRows.Close()
		if err != nil {
			log.Fatalf("err3: %v", err)
		}
	}(dataRows)

	// ("select url from %s", ct.chInputTablename) query gave errors. as following:
	//2024/06/28 13:34:59 err: code: 62, message: Syntax error: failed at position 17 ('%'): %s. Expected one of: table, table function, subquery or list of joined tables, table or subquery or table function, element of expression with optional alias, SELECT subquery, function, function name, compound identifier, list of elements, identifier, string literal table identifier
	//exit status 1

	for dataRows.Next() {
		var url string
		if err := dataRows.Scan(&url); err != nil {
			log.Fatalf("err %v \n", err)
		}
		inChan <- url
	}

	defer func(dataRows driver.Rows) {
		err := dataRows.Close()
		if err != nil {
			log.Fatalf("err14: %v", err)
		}
	}(dataRows)
	return nil
}

func (ct *chTransfer) DebugOutCh(ch1 chan *Response) {
	conn, err := connect()
	if err != nil {
		log.Fatal("connection error :%v \n", err)
	}

	ctx := context.Background()

	err = conn.Exec(ctx,
		"CREATE TABLE IF NOT EXISTS OutputTable(url String, status String,body_length Int)engine = MergeTree()order by status;")
	if err != nil {
		log.Fatalf("error12 : %v \n", err)
	}

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO OutputTable (url, status, body_length)")
	if err != nil {
		log.Fatalf("error7 : %v \n", err)
	}
	fmt.Println("\n\nDebug\n\n")
	for urlParsed := range ch1 {
		if err := batch.Append(urlParsed.Url, urlParsed.Status, urlParsed.Length); err != nil {
			log.Fatalf("error8 : %v \n", err)
		}
	}
	if err := batch.Send(); err != nil {
		log.Fatalf("error9 : %v \n", err)
	}
}

// batch, err := conn.PrepareBatch(ctx, "INSERT INTO OutputTable (url, status, body_length)")
//
//	if err != nil {
//		log.Fatalf("error7 : %v \n", err)
//	}
//
//	for urlParsed := range ch1 {
//		if err := batch.Append(urlParsed.Url, urlParsed.Status, urlParsed.Length); err != nil {
//			log.Fatalf("error8 : %v \n", err)
//		}
//	}
//
//	if err := batch.Send(); err != nil {
//		log.Fatalf("error9 : %v \n", err)
//	}
//
// }
func (ct *chTransfer) writeIntoDatabase(ch1 chan *Response) {

	ct.DebugOutCh(ch1)

	/*
		conn, err := connect()
		if err != nil {
			log.Fatalf("err5 %v", err)
		}

		ctx := context.Background()

		//rows, err := conn.Query(ctx, "CHECK_TABLE OutputTable")

		err = conn.Exec(ctx,
			"CREATE TABLE IF NOT EXISTS OutputTable(url String, status String,body_length Int)engine = MergeTree()order by status;")

		if err != nil {
			log.Fatalf("error6 : %v \n", err)
		}

		batch, err := conn.PrepareBatch(ctx, "INSERT INTO OutputTable (url, status, body_length)")
		if err != nil {
			log.Fatalf("error7 : %v \n", err)
		}
		for urlParsed := range ch1 {
			if err := batch.Append(urlParsed.Url, urlParsed.Status, urlParsed.Length); err != nil {
				log.Fatalf("error8 : %v \n", err)
			}
		}
		if err := batch.Send(); err != nil {
			log.Fatalf("error9 : %v \n", err)
		}
	*/

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
		log.Fatalf("err10: %v", err)
	}

	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return nil, err
	}
	return conn, nil
}
