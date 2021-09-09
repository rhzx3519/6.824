package crawler

import "fmt"

func worker(url string, ch chan []string, fetcher Fetcher) {
	fmt.Println("Crawler has fetched url:", url)
	urls, err := fetcher.Fetch(url)
	if err != nil {
		ch <- []string{}
	} else {
		ch <- urls
	}
}

func master(ch chan []string, fetcher Fetcher) {
	n := 1
	fetched := make(map[string]bool)
	for urls := range ch {
		for _, u := range urls {
			if !fetched[u] {
				fetched[u] = true
				n++
				// go worker
				go worker(u, ch, fetcher)
			}
		}
		n--
		if n == 0 {
			break
		}
	}

	fmt.Println("Crawler shut down...")
}

func ConcurrentChannel(url string, fetcher Fetcher) {
	ch := make(chan []string)
	go func() {
		ch <- []string{url}
	}()

	master(ch, fetcher)
}