package crawler

type Fetcher struct {

}

func (f Fetcher) Fetch(url string) (urls []string, err error) {
	urls = []string{"www.docs.google.com"}
	return
}