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

func transfer(ch1 chan string, url *URL) {
	conn, err := connect()
	if err != nil {
		panic((err))
	}

	ctx := context.Background()
	rows, err := conn.Query(ctx, "CHECK_TABLE URL_table")
	if err != nil {
		log.Fatalf("error : %v \n", err)
	}
	for rows.Next() {
		var result string
		var binary int
		if err := rows.Scan(&result, &binary); err != nil {
			log.Fatal(err)
		}

		if binary != 1 {
			rows, err := conn.Query(ctx, "create table URL_table("+
				"chHost String,"+
				"chUser String,"+
				"chPassword String,"+
				"chDatabasename String,"+
				"chInputTablename String,)"+
				"chOutputTablename String,"+
				"engine = MergeTree()order by id;")
			if err != nil {
				log.Fatalf(" rows:%v\n error:%v\n", rows, err)
			}
		}
	}
	for range ch1 {
		urlParsed := <-ch1
		query, err1 := conn.Query(ctx, "insert into url_table("+
			"chHost, chUser, chPassword, chDatabasename, chInputTablename, chOutputTablename)", urlParsed)
		if err != nil {
			log.Fatalf(" query:%v\n error:%v\n", query, err1)
		}

	}

	//conn.Query(ctx, "insert into my_table(id, name)\nvalues (1, '123');\ninsert into my_table(name, id)\nvalues ('abc', 2);\n")
	//conn.Query(ctx, "select *\nfrom my_table\nwhere id = 2;\nselect name, id\nfrom my_table;\n")
	if err != nil {
		log.Fatal(err)
	}

	for rows.Next() {
		var name, uuid string

		if err := rows.Scan(&name, &uuid); err != nil {
			log.Fatal(err)
		}

		log.Printf("name: %s, uuid: %s", name, uuid)
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
