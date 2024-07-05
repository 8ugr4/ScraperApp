package main

import "regexp"

type address struct {
	id     int
	ToF    bool
	url    string
	domain string //.com , .net , .org ... //root servers

}

const pattern string = `^(?:http(s)?://)?[\w.-]+(?:\.[\w\.-]+)+[\w\-\._~:/?#[\]@!$&'()*+,;=.]+$`

// IsValidURL takes the list of URL from chTransfer.readFromCh
// compares the url's with pattern, if it's valid returns true
// if it's not valid returns false
// but i have to connect the url with this value, how?
// add iota ?

func (a *address) IsValidURL(control chan string) bool {

	rgx := regexp.MustCompile(pattern)

	for urls := range control {
		if rgx.MatchString(urls) {

		}
	}

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

func bam() *address {
	return &address{
		url:    "a",
		domain: "a",
	}
}
