package test

import (
	"regexp"
)

type address struct {
	id     int
	ToF    bool // true or false??
	url    string
	domain string //.com , .net , .org ... //root servers

}

const pattern string = `^(?:http(s)?://)?[\w.-]+(?:\.[\w\.-]+)+[\w\-\._~:/?#[\]@!$&'()*+,;=.]+$`

// IsValidURL takes the list of URL from chTransfer.readFromCh
// compares the url's with pattern, if it's valid returns true
// if it's not valid returns false
// ,but I have to connect the url with this value, how?
// add iota ?

func IsValidURL(url string) bool {
	rgx := regexp.MustCompile(pattern)
	return rgx.MatchString(url)
}

func checkListOfURLs(urls []string) []string {

	var result []string

	for _, url := range urls {
		IsValidURL, _ := regexp.MatchString(pattern, url)
		if IsValidURL {
			result = append(result, url)
		}
	}
	return result
}

//func bam() *address {
//	return &address{
//		url:    "a",
//		domain: "a",
//	}
//}
