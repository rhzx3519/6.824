package goroutines

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func ExampleClosure() {
	var a string
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		a = "hello world"
		wg.Done()
	}()

	wg.Wait()
	fmt.Println(a)
	// Output:
	// hello world
}

func TestSendRPC(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(x int) {
			sendRPC(x)
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func sendRPC(i int) {
	fmt.Println(i)
}

// tick
func TestPeriodic(t *testing.T) {
	time.Sleep(1 * time.Second)
	fmt.Println("started...")
	go periodic()
	time.Sleep(5 * time.Second)	// wait for a while
}

func periodic() {
	for {
		fmt.Println("tick")
		time.Sleep(1 * time.Second)
	}
}

// sleep cancel
func TestSleepCancel(t *testing.T) {
	var mutex sync.Mutex
	var done bool

	time.Sleep(1 * time.Second)
	fmt.Println("started...")
	go periodicSleepCancel(mutex, &done)

	time.Sleep(5 * time.Second)
	mutex.Lock()
	done = true
	mutex.Unlock()
	fmt.Println("call tick cancelled")

	time.Sleep(3 * time.Second)
}

func periodicSleepCancel(mutex sync.Mutex, done *bool)  {
	for {
		fmt.Println("tick...")
		time.Sleep(1 * time.Second)
		mutex.Lock()
		if *done {
			fmt.Println("tick is cancelled")
			mutex.Unlock()
			return
		}
		mutex.Unlock()
	}
}

// vote count
//
// mu.Lock()
// cond.Broadcast()
// mu.Unlock()
// ----
// mu.Lock()
// while condition == false {
// 		cond.Wait()
// }
// mu.Unlock()
func TestVoteCount(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	count := 0
	finished := 0
	var mutex sync.Mutex
	cond := sync.NewCond(&mutex)

	for i := 0; i < 10; i++ {
		go func() {
			vote := requestVote()
			mutex.Lock()
			defer mutex.Unlock()
			if vote {
				count++
			}
			finished++
			cond.Broadcast()
		}()
	}

	mutex.Lock()
	for count < 5 && finished != 10 {
		cond.Wait()
	}
	if count >= 5 {
		fmt.Println("received 5+ votes!")
	} else {
		fmt.Println("lost")
	}

	mutex.Unlock()

}

func requestVote() bool {
	time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	return rand.Int()&1 == 0
}

// channel
func TestChanBlock(t *testing.T) {
	c := make(chan bool)
	go func() {
		time.Sleep(1 * time.Second)
		<- c
	}()
	start := time.Now()
	c <- true	// blocks until other goroutine receives
	fmt.Printf("send took %v\n", time.Since(start))
}

func TestDoWork(t *testing.T) {
	c := make(chan int)
	for i := 0; i < 4; i++ {
		go doWork(c)
	}
	for i := 0; i < 4; i++ {
		v := <- c
		fmt.Println(v)
	}
}

func doWork(c chan int) {
	for {
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
		c <- rand.Int()
	}
}

// wait
func TestWait(t *testing.T) {
	done := make(chan bool)
	for i := 0; i < 5; i++ {
		go func(x int) {
			sendRPC(x)
			done <- true
		}(i)
	}

	for i := 0; i < 5; i++ {
		<- done
	}
}






