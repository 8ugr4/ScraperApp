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

// using distinct will unique the column data

const (
	//inputTableCreateSQLQuery   = `CREATE TABLE IF NOT EXISTS InputTable(url String)engine = MergeTree()order by url`
	selectColumnSQLQuery       = "select distinct url from InputTable"
	dropTableSQLQuery          = "DROP TABLE IF EXISTS OutputTable"
	outputTableSQLQuery        = "CREATE TABLE IF NOT EXISTS OutputTable(url String, status String,body_length Int)engine = MergeTree()order by status"
	insertOutputTableSQLQuery  = "INSERT INTO OutputTable (url,status,body_length) VALUES (?, ?, ?) "
	controlOutputTableSQLQuery = "select * from OutputTable"
)

type chTransfer struct {
	*URL
}

func (ct *chTransfer) CountURLinCh(conn driver.Conn) int {

	ctx := context.Background()

	rows, err := conn.Query(ctx, fmt.Sprintf("%s", selectColumnSQLQuery))
	if err != nil {
		fmt.Printf("couldn't select from table: %q", err)
	}
	defer rows.Close()
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

func (ct *chTransfer) readFromCh(conn driver.Conn, inChan chan string) error {

	ctx := context.Background()

	rows, err := conn.Query(ctx, fmt.Sprintf("%s", selectColumnSQLQuery))
	if err != nil {
		return fmt.Errorf("couldn't select from table: %q", err)
	}
	defer rows.Close()

	for rows.Next() {

		var url string
		if err := rows.Scan(&url); err != nil {
			log.Fatalf("couldn't scan the rows :%v \n", err)
		}
		inChan <- url
	}
	return nil
}

func (ct *chTransfer) ControlOutput(conn driver.Conn) {
	ctx := context.Background()

	err := conn.Exec(ctx, fmt.Sprintf("%s", controlOutputTableSQLQuery))
	if err != nil {
		fmt.Printf("couldn't select from output table: %q \nor given table is empty", err)
	}

}
func (ct *chTransfer) DebugOutCh(wg *sync.WaitGroup, conn driver.Conn, ch1 chan *Response, count int) error {

	ctx := context.Background()
	defer wg.Done()

	if err := conn.Exec(ctx, fmt.Sprintf("%s", dropTableSQLQuery)); err != nil {
		fmt.Printf("%q\n", err)
	}

	err := conn.Exec(ctx, fmt.Sprintf("%s", outputTableSQLQuery))
	if err != nil {
		return fmt.Errorf("couldn't create table: %q \n", err)
	}
	fmt.Println("table created successfully.")

	//defer close(ch1)
	err1 := conn.Exec(ctx, "INSERT into OutputTable (url,status,body_length) values ('www.oyle.com','ok',22325)")
	if err1 != nil {
		return fmt.Errorf("couldn't insert into table %q\n", err1)
	}

	for i := 0; i < count-1; i++ {

		fmt.Printf("count :%d\n", count)

		Parsed := <-ch1

		fmt.Printf("%s\n %s\n %d\n", Parsed.Url, Parsed.Status, Parsed.Length)

		//values := fmt.Sprintf(" '%s' '%s' %d", Parsed.Url, Parsed.Status, Parsed.Length)
		//query := insertOutputTableSQLQuery + values

		err := conn.Exec(ctx, insertOutputTableSQLQuery, Parsed.Url, Parsed.Status, Parsed.Length)
		if err != nil {
			return fmt.Errorf("\ncould not append to the Table,\nERROR: : %q\n", err)
		}

		//fmt.Printf("url: %q\nstatus: %s\nlength: %d\n", Parsed.Url, Parsed.Status, Parsed.Length)

		//batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("%s", insertOutputTableSQLQuery))
		//if err != nil {
		//	return fmt.Errorf("couldn't prepare batch: %q \n", err)
		//}

		//err1 := batch.Append(Parsed.Url, Parsed.Status, Parsed.Length)
		//if err1 != nil {
		//	return fmt.Errorf("couldn't append to batch: %q \n", err)
		//}
	}

	return fmt.Errorf("ok")
}

func (ct *chTransfer) writeIntoCh(wg *sync.WaitGroup, conn driver.Conn, ch1 chan *Response, count int) error {
	err := ct.DebugOutCh(wg, conn, ch1, count)
	fmt.Printf("debug into ch worked:%q\n", err)

	return nil
}

func (u *URL) connect() (driver.Conn, error) {

	ctx := context.Background()

	host, port := u.chHost, u.chPort
	//b := string(fmt.Sprintf("%s:%s", host, port))
	//fmt.Printf(b) : localhost:9000
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%s", host, port)},
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
