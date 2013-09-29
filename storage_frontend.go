package main

import (
  "bytes"
  "errors"
  "flag"
  "fmt"
  "io/ioutil"
  "math"
  "math/rand"
  "net"
  "net/http"
  "os"
  "os/signal"
  "runtime"
  "sync"
  "time"
)

const MAX_CONTENT_LENGHT = 1024 // Maximum length of the content of the http request (1 kilobyte)
const MAX_STORAGE_SIZE = 104857600 // Maximum total storage allowed (100 megabytes)

const NUM_TESTS = 5000

type StorageServerFrontend struct {
  size uint
  storageBackendNodes []string
  cheatMap map[string][]byte
}

type StorageServerTest struct {
  host string
  keyValuePairs map[string]string
}

func (this *StorageServerFrontend) sendGET(key string) ([]byte, error) {
  return this.cheatMap[key], nil
}

func (this *StorageServerFrontend) sendPUT(key string, value []byte) error {
  this.cheatMap[key] = value
  
  return nil
}

func (this *StorageServerFrontend) ServeHTTP(w http.ResponseWriter, req *http.Request) {
  var header http.Header = w.Header()
  var key string = req.RequestURI
  var value []byte
  var err error
  
  defer req.Body.Close()
  
  switch (req.Method) {
  case "GET":
    value, err = this.sendGET(key)
    if err != nil {
      http.Error(w, fmt.Sprintf("Internal Error: %s", err.Error()), 500)
      return
    }
    
    if value == nil {
      http.Error(w, "Key not found", 404)
      return
    }
    
    header.Set("Content-Type", "application/octet-stream")
    w.Write(value)
  case "PUT":
    value, err = ioutil.ReadAll(req.Body)
    if err != nil {
      http.Error(w, "Unable to read request body", 500)
      return
    }
    
    size := uint(len(value))
    
    if size > MAX_CONTENT_LENGHT {
      http.Error(w, "Content body is too large", 400)
      return
    }
    
    this.size += size
    
    if this.size > MAX_STORAGE_SIZE {
      http.Error(w, "Storage server(s) exhausted", 500)
      return
    }
    
    err = this.sendPUT(key, value)
    if err != nil {
      http.Error(w, fmt.Sprintf("Internal Error: %s", err.Error()), 500)
      return
    }
    
    header.Set("Content-Type", "text/html")
  default:
    header.Set("Content-Type", "text/html")
    header.Set("Allow", "GET, PUT")
    http.Error(w, "Method Not Allowed", 405)
  }
}

func (this *StorageServerFrontend) Init(nodes []string) {
  this.cheatMap = make(map[string][]byte)
  this.storageBackendNodes = nodes
}

func randint(min, max int) int {
  rand.Seed(time.Now().UnixNano())
  return rand.Intn(max - min) + min
}

func choice(pool string) byte {
  return pool[rand.Intn(len(pool))]
}

func (this *StorageServerTest) generateKeyValuePair() (key, value string) {
  var buf []byte
  var n int
  
  n = randint(10, 20)
  buf = make([]byte, n)
  for i := 0; i < n; i++ {
    buf[i] = choice("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")
  }
  key = string(buf[0:n])
  
  n = randint(20, 40)
  buf = make([]byte, n)
  for i := 0; i < n; i++ {
    buf[i] = choice("1234567890")
  }
  value = string(buf[0:n])
  
  return
}

func (this *StorageServerTest) putTestObject(key, value string) (bool, error) {
  fmt.Println("PUT(key, value):", key, value)
  
  req, err := http.NewRequest("PUT", this.host + "/" + key, bytes.NewBufferString(value))
  if err != nil {
    return false, err
  }
  
  resp, err := http.DefaultClient.Do(req)
  if err != nil {
    return false, err
  }
  
  defer resp.Body.Close()
  
  if resp.StatusCode != 200 {
    return false, errors.New(fmt.Sprintf("Got HTTP status code %d", resp.StatusCode))
  }
  
  return true, nil
}

func (this *StorageServerTest) getTestObject(key, value string) (bool, error) {
  fmt.Println("GET(key, value):", key, value)
  
  req, err := http.NewRequest("GET", this.host + "/" + key, nil)
  if err != nil {
    return false, err
  }
  
  resp, err := http.DefaultClient.Do(req)
  if err != nil {
    return false, err
  }
  
  defer resp.Body.Close()
  
  if resp.StatusCode != 200 {
    return false, errors.New(fmt.Sprintf("Got HTTP status code %d", resp.StatusCode))
  }
  
  body, err := ioutil.ReadAll(resp.Body)
  if err != nil {
    return false, err
  }
  
  retrievedValue := string(body)
  
  if value != retrievedValue {
    fmt.Println("Value is not equal to retrieved value", value, "!=", retrievedValue)
    return false, nil
  }
  
  return true, nil
}

func (this *StorageServerTest) Run(host string, testsToRun int, done chan bool) bool {
  var key, value string
  var contains bool
  
  this.host = host
  this.keyValuePairs = make(map[string]string)
  
  fmt.Println("Generating data...")
  
  // Generate random unique key, value pairs
  for i := 0; i < testsToRun; i++ {
    select {
    case <- done:
      return true
    default:
      for {
        key, value = this.generateKeyValuePair()
        _, contains = this.keyValuePairs[key]
        if !contains {
          break
        }
      }
      this.keyValuePairs[key] = value
      runtime.Gosched()
    }
  }
  
  // Call put to insert the key/value pairs
  for key, value = range this.keyValuePairs {
    select {
    case <- done:
      return true
    default:
      ok, err := this.putTestObject(key, value)
      if err != nil {
        fmt.Println("Failed to put", key, value, "with error", err.Error())
        return false
      }
      
      if !ok {
        fmt.Println("Error putting", key, value)
        return false
      }
    }
  }
  
  // Validate that all key/value pairs are found
  for key, value = range this.keyValuePairs {
    select {
    case <- done:
      return true
    default:
      ok, err := this.getTestObject(key, value)
      if err != nil {
        fmt.Println("Failed to get", key, value, "with error", err.Error())
        return false
      }
      
      if !ok {
        fmt.Println("Error getting", key, value)
        return false
      }
    }
  }
  
  return true
}

func main() {
  var err error
  var runTests bool
  var httpServerPort uint
  var done chan bool
  var wg *sync.WaitGroup
  var frontend StorageServerFrontend
  var tests StorageServerTest
  var listener net.Listener
  
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
  
  if httpServerPort > math.MaxUint16 {
    fmt.Println("Invalid port %d", httpServerPort)
    flag.Usage()
    os.Exit(2)
  }
  
  done = make(chan bool)
  
  frontend.Init(flag.Args())
  http.Handle("/", &frontend)
  
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
  
  wg = new(sync.WaitGroup)
  
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
      result := tests.Run(fmt.Sprintf("http://localhost:%d", httpServerPort), NUM_TESTS, done)
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


