package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"log"
)

// using distinct will unique the column data

const (
	inputTableCreateSQLQuery  = "CREATE TABLE IF NOT EXISTS InputTable(url String)engine = MergeTree()order by url"
	selectColumnSQLQuery      = "select distinct url from InputTable"
	outputTableSQLQuery       = "CREATE TABLE IF NOT EXISTS OutputTable(url String, status String,body_length Int)engine = MergeTree()order by status;"
	insertOutputTableSQLQuery = "INSERT INTO OutputTable (url, status, body_length)"
)

type chTransfer struct {
	*URL
}

func (ct *chTransfer) DebugInCh(conn driver.Conn) error {

	ctx := context.Background()

	err := conn.Exec(ctx, fmt.Sprintf("%s", inputTableCreateSQLQuery))
	if err != nil {
		return fmt.Errorf("couldn't create table : %q \n", err)
	}
	fmt.Println("end of Debug")

	rows, err := conn.Query(ctx, fmt.Sprintf("%s", selectColumnSQLQuery))
	if err != nil {
		return fmt.Errorf("couldn't select from table: %q", err)
	}
	defer rows.Close()

	for rows.Next() {
		var url string
		if err := rows.Scan(&url); err != nil {
			return fmt.Errorf("couldn't scan from table %q \n", err)
		}
		fmt.Printf("url : %v\n", url)
	}
	return nil
}

func (ct *chTransfer) readFromClick(conn driver.Conn, inChan chan string) error {

	ctx := context.Background()

	rows, err := conn.Query(ctx, fmt.Sprintf("%s", selectColumnSQLQuery))
	if err != nil {
		return fmt.Errorf("couldn't select from table: %q", err)
	}
	defer rows.Close()

	for rows.Next() {
		var url string
		if err := rows.Scan(&url); err != nil {
			log.Fatalf("err %v \n", err)
		}
		inChan <- url
	}

	return nil
}

func (ct *chTransfer) DebugOutCh(conn driver.Conn, ch1 chan *Response) error {

	ctx := context.Background()

	err := conn.Exec(ctx, fmt.Sprintf("%s", outputTableSQLQuery))
	if err != nil {
		return fmt.Errorf("couldn't create table: %q \n", err)
	}

	batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("%s", insertOutputTableSQLQuery))
	if err != nil {
		return fmt.Errorf("couldn't insert the data into the output_table : %q \n", err)
	}

	fmt.Println("\nDebug")

	for urlParsed := range ch1 {
		fmt.Printf("urlParsed.Url:%v\n", urlParsed.Url)
		if err := batch.Append(urlParsed.Url, urlParsed.Status, urlParsed.Length); err != nil {
			return fmt.Errorf("couldn't append into the table /batch : %q \n", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("couldn't send the data /batch : %q \n", err)
	}
	return nil
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
func (ct *chTransfer) writeIntoDatabase(conn driver.Conn, ch1 chan *Response) error {

	err := ct.DebugOutCh(conn, ch1)
	if err != nil {
		return fmt.Errorf("couldn't call DebugOutCh function.\n")
	}

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
	return nil
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
		var exception *clickhouse.Exception
		if errors.As(err, &exception) {
			fmt.Printf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return nil, err
	}
	return conn, nil
}
