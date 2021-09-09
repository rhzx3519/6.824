package crawler

import "testing"

func TestConcurrentChannel(t *testing.T) {
	fetcher := Fetcher{}
	ConcurrentChannel("www.google.com", fetcher)
}
