package main

import (
  "flag"
  "fmt"
  "math"
  "net"
  "net/http"
  "os"
  "os/signal"
  "sync"
  "github.com/egraff/inf-3200-1-frontend/frontend"
  "github.com/egraff/inf-3200-1-frontend/frontendtest"
)

const NUM_TESTS = 1000

type DHTFrontendHandler struct {
  storageBackendNodes []string
}

func (this *DHTFrontendHandler) GET(key string) ([]byte, error) {
  // Implement me!!
  
  return nil, nil
}

func (this *DHTFrontendHandler) PUT(key string, value []byte) error {
  // Implement me!!
  
  return nil
}

func main() {
  var err error
  var runTests bool
  var httpServerPort uint
  var listener net.Listener
  var handler frontend.StorageServerFrontendHandler
  
  flag.Usage = func() {
    fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
    flag.PrintDefaults()
    fmt.Fprintf(os.Stderr, "  compute-1-1 [compute-1-2 ... compute-N-M]\n")
  }
  
  flag.UintVar(&httpServerPort, "port", 8000, "portnumber(default=8000)")
  flag.BoolVar(&runTests, "runtests", false, "")
  
  flag.Parse()
  nodes := flag.Args()
  if len(nodes) == 0 {
    flag.Usage()
    os.Exit(1)
  }
  
  
  /************************************************************
   ** When you have implemented a proper handler, you should **
   ** comment in the line below. As long as the line is      **
   ** commented out, the frontend will use a local hashmap   **
   ** to implement the key-value database, and the tests     **
   ** pass!
   ***********************************************************/
  //handler = &DHTFrontendHandler{nodes}
  
  
  
  if httpServerPort > math.MaxUint16 {
    fmt.Println("Invalid port %d", httpServerPort)
    flag.Usage()
    os.Exit(2)
  }
  
  wg := new(sync.WaitGroup)
  done := make(chan bool)
  frontend := frontend.New(handler)
  http.Handle("/", frontend)
  
  listener, err = net.Listen("tcp", fmt.Sprintf(":%d", httpServerPort))
  if err != nil {
    fmt.Println("Failed to listen on port", httpServerPort, "with error", err.Error())
    os.Exit(3)
  }
  
  go func() {
    c := make(chan os.Signal)
    signal.Notify(c, os.Interrupt)
    <-c
    listener.Close()
    close(done)
  }()
  
  wg.Add(1)
  go func() {
    defer wg.Done()
    http.Serve(listener, nil)
  }()
  
  if runTests {
    fmt.Println("Running tests...")
    
    wg.Add(1)
    go func() {
      defer wg.Done()
      result := frontendtest.Run(fmt.Sprintf("http://localhost:%d", httpServerPort), NUM_TESTS, done)
      if result {
        fmt.Println("Test passed!")
      } else {
        fmt.Println("Test failed!")
      }
      
      listener.Close()
    }()
  }
  
  wg.Wait()
  fmt.Println("Bye, bye!")
}
