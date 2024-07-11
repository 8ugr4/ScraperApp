package main

import (
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"io"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"
	"web-scraper-go/pkg/test"
)

type URL struct {
	conn          driver.Conn
	url           string
	chHost        string
	chPort        int
	chUser        string
	chPassword    string
	chDatabase    string
	chInputTable  string
	chOutputTable string
	errorMsg      string
	countOfUrl    int
}

type Response struct {
	Status string
	Url    string
	Length int64
}

var (
	httpClient = &http.Client{
		Timeout: 30 * time.Second,
	}
)

// as default, chHost:localhost, chPort:9000 ... as follows.

func New(chPort int, chHost, chUser, chPassword, chDatabase, chInputTable, chOutputTable string) *URL {
	return &URL{
		chHost:        chHost,
		chPort:        chPort,
		chUser:        chUser,
		chPassword:    chPassword,
		chDatabase:    chDatabase,
		chInputTable:  chInputTable,
		chOutputTable: chOutputTable,
	}
}

// connects chTransfer functions to main file.
// calls the necessary functions to read from given InputTableName (Ch),
// parses them, writes them into given OutputTableName into Clickhouse.

func (u *URL) Run(wg *sync.WaitGroup, workersCnt int) {

	ct := chTransfer{URL: &URL{
		chHost: u.chHost,
		chPort: u.chPort,
	}}

	conn, err := ct.connect()
	if err != nil {
		fmt.Printf("ERROR : %q\n", err)
	}

	// takes the row count of the InputTable.

	u.countOfUrl = ct.CountRows(conn)

	fmt.Printf("there are %d urls and %d workers\n", u.countOfUrl, workersCnt)

	defer func(start time.Time) {
		fmt.Printf("it took %v to finish the Run()", time.Since(start))
	}(time.Now())

	// URL channel.
	urlStrCh := make(chan string, workersCnt)

	// control channel
	controlCh := make(chan string, u.countOfUrl)

	// Parsed URL channel.
	parseCh := make(chan *Response, u.countOfUrl)

	go u.readFile(conn, urlStrCh, controlCh)

	for workerId := 0; workerId < workersCnt; workerId++ {
		wg.Add(1)
		go u.httpWorker(wg, urlStrCh, parseCh)
	}

	u.writeIntoFile(wg, conn, parseCh, u.countOfUrl)

	defer wg.Done()
}

// reads the InputTable from Clickhouse

func (u *URL) readFile(conn driver.Conn, urlStrCh chan string, controlCh chan string) {

	ct := chTransfer{URL: &URL{}}

	err := ct.readFromCh(conn, urlStrCh, controlCh)
	if err != nil {
		log.Fatalf("could not read a line from the database: %v", err)
	}

}

// Parses URL's ( at the moment : status, length)

func (u *URL) parseUrl(urlStr string) *Response {

	defer func(start time.Time) {
		//log.Printf("scraping:%v\t,duration:%v", urlStr, time.Since(start))
		//log.Printf("usedWorkerCnt in Total is:%d", cnt)
	}(time.Now())

	request, err := http.NewRequest(http.MethodGet, urlStr, nil) //  GETS A RESPONSE
	if err != nil {
		log.Fatalln("Error creating request:", err)
	}

	request.URL, err = url.Parse(urlStr)
	if err != nil {
		log.Fatalln("Error parsing URL:", err)
	}
	resp := Response{
		Url: urlStr,
	} //new Response instance

	httpResp, err := httpClient.Do(request)
	if err != nil {
		log.Fatalf("error when sending request to the server: %v", err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Fatalln("Error closing Response body:", err)
		}
	}(httpResp.Body)

	resp.Status = httpResp.Status
	if httpResp.ContentLength == -1 {
		body, err := io.ReadAll(httpResp.Body)
		resp.Length = int64(len(body))
		if err != nil {
			log.Fatalln("Error reading Response body:", err)
		}
	} else {
		resp.Length = httpResp.ContentLength
	}
	return &resp
}

// reads from UrlStrChannel, sends the URL's to be parsed.

func (u *URL) httpWorker(wg *sync.WaitGroup, urlStrCh <-chan string, parseCh chan<- *Response) {

	defer wg.Done()

	for urlStr := range urlStrCh {
		if test.IsValidURL(urlStr) {
			parseCh <- u.parseUrl(urlStr)
		}
	}
}

// writeIntoFile sends the URL's to ch.WriteIntoCh
// reads from parseChannel (urls)

func (u *URL) writeIntoFile(wg *sync.WaitGroup, conn driver.Conn, parseCh <-chan *Response, count int) {

	// channel to send Parsed URL's as a struct. (see : Response )
	ch1 := make(chan *Response)

	go func() {
		for urlStr := range parseCh {
			ch1 <- urlStr
		}
	}()

	ct := chTransfer{
		URL: &URL{}}

	err := ct.writeIntoCh(wg, conn, ch1, count)

	if err != nil {
		fmt.Printf("couldn't connect to the database :%q\n", err)
	}
}

func main() {

	workersCnt := 4

	sc1 := New(9000, "localhost", "default", "", "chDatabase", "urls_to_parse", "url_parse_results")

	var wg sync.WaitGroup
	for i := 0; i < workersCnt; i++ {
		wg.Add(1)
	}

	sc1.Run(&wg, workersCnt)
}

//https://www.youtube.com/watch?v=V-VRVWdAUgA
//https://www.youtube.com/watch?v=6JcPRFEENVs
//https://dev.to/adriandy89/understanding-golang-object-oriented-programming-oop-with-examples-15l6
