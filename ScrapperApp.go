package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

type scrape interface {
	control()
}
type Scrapper struct {
	inputFile  string
	outputFile string
	workersCnt int
	channels   string
}

func startScraping(files ...scrape) {
	for _, file := range files {
		file.control()
	}
}

var workersCnt = runtime.NumCPU()

func New(inputFile, outputFile string, workersCnt int) *Scrapper {
	if !strings.HasPrefix(inputFile, "C:/") || !strings.HasPrefix(outputFile, "C:/") {
		log.Fatalf("ERROR: Filepath must start with 'C:/'\ngiven paths are:"+
			"\ninputFilepath: %s\noutputFilepath: %s\n", inputFile, outputFile)
	}

	return &Scrapper{
		inputFile:  inputFile,
		outputFile: outputFile,
		workersCnt: workersCnt,
	}
}

func (s *Scrapper) Run() { //method of obj Scrapper
	defer func(start time.Time) {
		log.Printf("it took %v to finish the Run()", time.Since(start))
	}(time.Now())

	log.Printf("using %d workers", workersCnt)

	urlStrCh := make(chan string, workersCnt) // urls channel
	parseCh := make(chan *response, 3)        // parsed channel
	errCh := make(chan error, 1)              //error channel

	var workerWg sync.WaitGroup

	// reads file from filePath and sends the inputs
	go s.readFile(urlStrCh)
	go func() {
		time.Sleep(2 * time.Millisecond)
		select {
		case msg1 := <-errCh:
			if msg1 != nil {
				log.Println(msg1)
			}
		}
	}()
	//into urlStrCh channel as it reads.
	for workerId := 0; workerId < workersCnt; workerId++ {
		workerWg.Add(1)
		go s.httpWorker(workerId, &workerWg, urlStrCh, parseCh)
	}

	//reads from urlStrCh and writes into .csv data.
	go s.writeIntoFile(parseCh)

	workerWg.Wait()
	close(errCh)
}

type response struct {
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
func (s *Scrapper) readFile(urlStrCh chan<- string) {

	inputFp, err := os.Open(s.inputFile) //inputFilePointer
	if err != nil {
		log.Fatalln("Error opening file:", err)
	}
	defer func() {
		err := inputFp.Close()
		if err != nil {
			log.Fatalln("Error closing file:", err)
		}
	}()

	sc := bufio.NewScanner(inputFp)
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
func (s *Scrapper) parseUrl(workerId int, urlStr string) *response {

	defer func(start time.Time) {
		log.Printf("worker:%d\tscraping:%v\t,duration:%v", workerId, urlStr, time.Since(start))
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
	resp := response{
		Url: urlStr,
	} //new response instance

	httpResp, err := httpClient.Do(request)
	if err != nil {
		log.Fatalf("error when sending request to the server: %v", err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Fatalln("Error closing response body:", err)
		}
	}(httpResp.Body)

	resp.Status = httpResp.Status
	if httpResp.ContentLength == -1 {
		body, err := io.ReadAll(httpResp.Body)
		resp.Length = int64(len(body))
		if err != nil {
			log.Fatalln("Error reading response body:", err)
		}
	} else {
		resp.Length = httpResp.ContentLength
	}
	return &resp
}

func (s *Scrapper) httpWorker(workerId int, wg *sync.WaitGroup, urlStrCh <-chan string, parseCh chan<- *response) {
	defer wg.Done()
	for urlStr := range urlStrCh {
		parseCh <- s.parseUrl(workerId, urlStr)
	}

}

// writeIntoFile writes the parsed Input(URL) to a file.
// reads from parsedFlowReceiver puts the inputs into a .csv file.

func (s *Scrapper) writeIntoFile(parseCh <-chan *response) {
	//"scrapedFile.csv" changed with newFilepath
	fp, err := os.Create(s.outputFile)
	if err != nil {
		log.Fatalln("Error creating file:", err)
	}
	defer func() {
		if err := fp.Close(); err != nil {
			log.Fatalln("Error closing file:", err)
		}
	}()

	for resp := range parseCh {
		_, err := fmt.Fprintf(fp, "%v: %v: %v\n", resp.Status, resp.Url, resp.Length)
		if err != nil {
			log.Fatalln("Error writing response:", err)
		} // todo check for the error
		if _, err := fp.Write([]byte(resp.Url)); err != nil {
			log.Fatalf("could not write into the file %v\n", err)
		}
	}
}

// readFileWg := &sync.WaitGroup{}, use : readFileWg
// var readFileWg sync.WaitGroup, use :  &readFileWg
func main() {
	inputFile := "C:/Users/Bugra/Desktop/listOfUrl.txt"
	outputFile := "C:/Users/Bugra/Desktop/reportOfUrl1.txt"
	newScrapper := dnsResolver{
		files: files{},
	}
	startScraping(newScrapper)

	myScrapper := New(inputFile, outputFile, workersCnt)
	myScrapper.Run()

}
