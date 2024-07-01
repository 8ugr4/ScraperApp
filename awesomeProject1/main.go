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
)

type URL struct {
	url               string
	chHost            string
	chUser            string
	chPassword        string
	chDatabasename    string
	chInputTablename  string
	chOutputTablename string
}

//var workersCnt = runtime.NumCPU()

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

func New(chHost, chUser, chPassword, chDatabasename, chInputTablename, chOutputTablename string) *URL {
	return &URL{
		chHost:            chHost,
		chUser:            chUser,
		chPassword:        chPassword,
		chDatabasename:    chDatabasename,
		chInputTablename:  chInputTablename,
		chOutputTablename: chOutputTablename,
	}
}

func (r *Response) Run(conn driver.Conn, workersCnt int) { //method of obj Scrapper
	defer func(start time.Time) {
		log.Printf("it took %v to finish the Run()", time.Since(start))
	}(time.Now())

	log.Printf("using %d workers", workersCnt)

	urlStrCh := make(chan string, workersCnt)   // urls channel
	parseCh := make(chan *Response, workersCnt) // parsed channel

	var workerWg sync.WaitGroup

	// reads file from Database table and sends the inputs

	go r.readFile(conn, urlStrCh)

	//into urlStrCh channel as it reads.
	for workerId := 0; workerId < workersCnt; workerId++ {
		workerWg.Add(1)
		go r.httpWorker(&workerWg, urlStrCh, parseCh)
	}

	go func() {
		workerWg.Wait()
		close(parseCh)
	}()
	//reads from urlStrCh and sends data into writeIntoCh channel.
	r.writeIntoFile(conn, parseCh)
}

// readFile reads the file from filePath, and while reading sends the url input to the urlFlowSender channel.
func (r *Response) readFile(conn driver.Conn, urlStrCh chan string) {
	//implement reading from database on clickhouse

	ct := chTransfer{
		URL: &URL{
			chInputTablename: "urls_to_parse",
		},
	}
	err := ct.readFromCh(conn, urlStrCh)
	if err != nil {
		log.Fatalf("could not read a line from the database: %v", err)
	}

	for a := range urlStrCh {
		fmt.Println(a)
	}

	close(urlStrCh)
}

func (r *Response) parseUrl(urlStr string) *Response {

	defer func(start time.Time) {
		log.Printf("scraping:%v\t,duration:%v", urlStr, time.Since(start))
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

func (r *Response) httpWorker(wg *sync.WaitGroup, urlStrCh <-chan string, parseCh chan<- *Response) {
	defer wg.Done()
	for urlStr := range urlStrCh {
		//fmt.Println("urlStr:%v", urlStr)
		parseCh <- r.parseUrl(urlStr)
	}

}

// writeIntoFile writes the parsed Input(URL) to a file.
// reads from parsedFlowReceiver puts the inputs into a .csv file.

func (r *Response) writeIntoFile(conn driver.Conn, parseCh <-chan *Response) {
	ch1 := make(chan *Response)
	go func() {
		for urlStr := range parseCh {
			ch1 <- urlStr
		}
		close(ch1)
	}()

	ct := chTransfer{
		URL: &URL{
			chOutputTablename: "OutputTable",
		},
	}
	err := ct.writeIntoCh(conn, ch1)
	if err != nil {
		fmt.Printf("couldn't connect to the database :%q\n", err)
	}
	// then from here call the chTransfer function
	// so that it'll writeIntoCh the given structure
	// inside the clickhouse table.
}

// readFileWg := &sync.WaitGroup{}, use : readFileWg
// var readFileWg sync.WaitGroup, use :  &readFileWg

func main() {
	workersCnt := 4
	//in := URL.fillIn()
	sc := &Response{}
	conn, err := connect()
	if err != nil {
		log.Fatalln("Error connecting to database:", err)
	}
	sc.Run(conn, workersCnt)
}
