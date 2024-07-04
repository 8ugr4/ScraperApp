package main

import (
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"
)

type URL struct {
	conn              driver.Conn
	url               string
	chHost            string
	chPort            string
	chUser            string
	chPassword        string
	chDatabasename    string
	chInputTablename  string
	chOutputTablename string
	errorMsg          string
	countOfUrl        int
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

func New(chHost, chPort, chUser, chPassword, chDatabasename, chInputTablename, chOutputTablename string) *URL {
	return &URL{
		chHost:            chHost,
		chPort:            chPort,
		chUser:            chUser,
		chPassword:        chPassword,
		chDatabasename:    chDatabasename,
		chInputTablename:  chInputTablename,
		chOutputTablename: chOutputTablename,
	}
}

func (u *URL) CheckHostPort() {
	fmt.Printf("url_host:%q\turl_port:%q\n", u.chHost, u.chPort)
}

//func (u *URL) Run(wg *sync.WaitGroup, conn driver.Conn, workersCnt int) {
//	(*Response).Run1(wg, conn, workersCnt)
//
//}

func (u *URL) Run(wg *sync.WaitGroup, conn driver.Conn, workersCnt int) { //method of obj Scrapper
	ct := chTransfer{
		URL: &URL{
			chInputTablename:  "urls_to_parse",
			chHost:            "localhost",
			chPort:            "9000",
			chOutputTablename: "OutputTable",
		},
	}
	u.countOfUrl = ct.CountURLinCh(conn)

	fmt.Printf("there are %d urls.\n", u.countOfUrl)

	defer func(start time.Time) {
		log.Printf("it took %v to finish the Run()", time.Since(start))
	}(time.Now())

	log.Printf("using %d workers", workersCnt)

	urlStrCh := make(chan string, workersCnt)     // urls channel
	parseCh := make(chan *Response, u.countOfUrl) // parsed channel
	// reads file from Database table and sends the inputs
	//into urlStrCh channel as it reads.
	if conn == nil {
		fmt.Println("conn is nil")
		os.Exit(1)
	}
	go u.readFile(conn, urlStrCh)

	for workerId := 0; workerId < workersCnt; workerId++ {
		wg.Add(1)
		go u.httpWorker(wg, urlStrCh, parseCh)
	}

	go func() {
		wg.Add(1)
		wg.Wait()
	}()
	//reads from urlStrCh and sends data into writeIntoCh channel.
	fmt.Println("before sending data to writeIntoFile function.")

	u.writeIntoFile(wg, conn, parseCh, u.countOfUrl)

	fmt.Println("\nafter sending data to writeIntoFile function.")
	//defer close(parseCh)
	defer wg.Done()
	//defer close(parseCh)
	//defer close(urlStrCh)
	fmt.Printf("probably it lies here....\n")
}

// readFile reads the file from filePath, and while reading sends the url input to the urlFlowSender channel.
func (u *URL) readFile(conn driver.Conn, urlStrCh chan string) {
	//implement reading from database on clickhouse

	ct := chTransfer{
		URL: &URL{
			chInputTablename:  "urls_to_parse",
			chHost:            "localhost",
			chPort:            "9000",
			chOutputTablename: "OutputTable",
		},
	}
	err := ct.readFromCh(conn, urlStrCh)
	if err != nil {
		log.Fatalf("could not read a line from the database: %v", err)
	}

}

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

func (u *URL) httpWorker(wg *sync.WaitGroup, urlStrCh <-chan string, parseCh chan<- *Response) {
	defer wg.Done()
	for urlStr := range urlStrCh {
		//fmt.Println("urlStr:%v", urlStr)
		parseCh <- u.parseUrl(urlStr)
		//fmt.Printf("%q\n", urlStr) //control for the urls
	}

}

// writeIntoFile writes the parsed Input(URL) to a file.
// reads from parsedFlowReceiver puts the inputs into a .csv file.

func (u *URL) writeIntoFile(wg *sync.WaitGroup, conn driver.Conn, parseCh <-chan *Response, count int) {
	ch1 := make(chan *Response)
	//defer close(ch1)
	go func() {
		for urlStr := range parseCh {
			ch1 <- urlStr
		}
	}()
	//defer close(ch1)

	ct := chTransfer{
		URL: &URL{
			chHost:            "localhost",
			chPort:            "9000",
			chOutputTablename: "OutputTable",
		},
	}
	err := ct.writeIntoCh(wg, conn, ch1, count)
	if err != nil {
		fmt.Printf("couldn't connect to the database :%q\n", err)
	} else {
		//fmt.Println("write into ch ended successfully.")
	}
	// then from here call the chTransfer function
	// so that it'll writeIntoCh the given structure
	// inside the clickhouse table.
}

// readFileWg := &sync.WaitGroup{}, use : readFileWg
// var readFileWg sync.WaitGroup, use :  &readFileWg

func main() {
	workersCnt := 4
	var wg sync.WaitGroup
	var conn driver.Conn

	sc1 := New("localhost", "9000", "default", "", "chDatabasename", "urls_to_parse", "url_parse_results")
	//fmt.Printf("sc1_host:%q\tsc1_port:%q\n", sc1.chHost, sc1.chPort)
	conn, err := sc1.connect()
	fmt.Printf("%q\n", err)

	//func1.CheckHostPort(sc1)

	for i := 0; i < workersCnt; i++ {
		wg.Add(1)
	}
	//https://www.youtube.com/watch?v=V-VRVWdAUgA
	//https://www.youtube.com/watch?v=6JcPRFEENVs
	//https://dev.to/adriandy89/understanding-golang-object-oriented-programming-oop-with-examples-15l6

	sc1.Run(&wg, conn, workersCnt)
}
