package main

import (
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

type scrape interface {
	control()
	transfer()
}

type URL struct {
	url,
	chHost,
	chUser,
	chPassword,
	chDatabasename,
	chInputTablename,
	chOutputTablename string
}

func startScraping(files ...scrape) {
	for _, file := range files {
		file.control()
		file.transfer()
	}
}

//var workersCnt = runtime.NumCPU()

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

func (u *URL) Run(a int) { //method of obj Scrapper

	defer func(start time.Time) {
		log.Printf("it took %v to finish the Run()", time.Since(start))
	}(time.Now())

	log.Printf("using %d workers", a)

	urlStrCh := make(chan string, a)   // urls channel
	parseCh := make(chan *Response, a) // parsed channel

	var workerWg sync.WaitGroup

	// reads file from Database table and sends the inputs

	go u.readFile(urlStrCh)

	//into urlStrCh channel as it reads.
	for workerId := 0; workerId < a; workerId++ {
		workerWg.Add(1)
		go u.httpWorker(workerId, &workerWg, urlStrCh, parseCh)
	}

	//reads from urlStrCh and sends data into writeIntoDatabase channel.
	go u.writeIntoFile(parseCh, a)

	workerWg.Wait()

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

// readFile reads the file from filePath, and while reading sends the url input to the urlFlowSender channel.
func (u *URL) readFile(urlStrCh chan string) {
	//implement reading from database on clickhouse
	u.readFromClick(urlStrCh)
	for sc.Scan() {
		urlStr := sc.Text()
		if urlStr == "" {
			log.Fatalln("Error scanning input:", err)
		} else if !strings.HasPrefix(urlStr, "https://") {
			log.Printf("this input is not an URL:%v\t", urlStr)
			//programi kapatmak yerine bir sonraki url checklemeye gecmesi gerekiyor.
		}
		urlStrCh <- urlStr
	}
	if err := sc.Err(); err != nil {
		log.Fatalf("could not read a line from the file: %v", err)
	}
	close(urlStrCh)
}

// important. always close the channel if other functions also use it, but wait for it to finish.
func (u *URL) parseUrl(urlStr string) *Response {

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

func (u *URL) httpWorker(wg *sync.WaitGroup, urlStrCh <-chan string, parseCh chan<- *Response) {
	defer wg.Done()
	for urlStr := range urlStrCh {
		parseCh <- u.parseUrl(urlStr)
	}

}

// writeIntoFile writes the parsed Input(URL) to a file.
// reads from parsedFlowReceiver puts the inputs into a .csv file.

func (r *Response) writeIntoFile(parseCh <-chan *Response) {
	ch1 := make(chan *Response)
	for urlStr := range parseCh {
		ch1 <- urlStr
	}
	r.writeIntoDatabase(ch1)
	// then from here call the chTransfer function
	// so that it'll writeIntoDatabase the given structure
	// inside the clickhouse table.
}

// readFileWg := &sync.WaitGroup{}, use : readFileWg
// var readFileWg sync.WaitGroup, use :  &readFileWg
func main() {
	newScrapper := dnsResolver{
		files:     files{},
		directory: Directory{},
		chTransfer: chTransfer{URL: &URL{
			chHost:            "chHost",
			chUser:            "chUser",
			chPassword:        "chPassword",
			chDatabasename:    "chDatabasename",
			chInputTablename:  "chInputTablename",
			chOutputTablename: "chOutputTablename",
		}},
	}
	startScraping(newScrapper)
	workersCnt := 4
	myScrapper := New(chHost, chUser, chPassword, chDatabasename, chInputTablename, chOutputTablename)
	myScrapper.Run(workersCnt)

}

//
//func (s *Scrapper) writeIntoFile(parseCh <-chan *Response) {
//	//"scrapedFile.csv" changed with newFilepath
//	fp, err := os.Create(s.outputFile)
//	if err != nil {
//		log.Fatalln("Error creating file:", err)
//	}
//	defer func() {
//		if err := fp.Close(); err != nil {
//			log.Fatalln("Error closing file:", err)
//		}
//	}()
//
//	for resp := range parseCh {
//		_, err := fmt.Fprintf(fp, "%v: %v: %v\n", resp.Status, resp.Url, resp.Length)
//		if err != nil {
//			log.Fatalln("Error writing Response:", err)
//		} // todo check for the error
//		if _, err := fp.Write([]byte(resp.Url)); err != nil {
//			log.Fatalf("could not write into the file %v\n", err)
//		}
//	}
//}
//
//func New(inputFile, outputFile string, workersCnt int) *Scrapper {
//	if !strings.HasPrefix(inputFile, "C:/") || !strings.HasPrefix(outputFile, "C:/") {
//		log.Fatalf("ERROR: Filepath must start with 'C:/'\ngiven paths are:"+
//			"\ninputFilepath: %s\noutputFilepath: %s\n", inputFile, outputFile)
//	}
//
//	return &Scrapper{
//		inputFile:  inputFile,
//		outputFile: outputFile,
//		workersCnt: workersCnt,
//	}
//}
