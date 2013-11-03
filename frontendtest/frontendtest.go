package frontendtest

import (
  "bytes"
  "errors"
  "fmt"
  "io/ioutil"
  "math/rand"
  "net/http"
  "runtime"
  "time"
)

type storageServerTest struct {
  host string
  keyValuePairs map[string]string
}

func randint(min, max int) int {
  rand.Seed(time.Now().UnixNano())
  return rand.Intn(max - min) + min
}

func choice(pool string) byte {
  return pool[rand.Intn(len(pool))]
}

func (this *storageServerTest) generateKeyValuePair() (key, value string) {
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

func (this *storageServerTest) putTestObject(key, value string) (bool, error) {
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
    fmt.Println("storageServerTest.putTestObject :: Got HTTP status code", resp.StatusCode)
    return false, errors.New(fmt.Sprintf("Got HTTP status code %d", resp.StatusCode))
  }
  
  return true, nil
}

func (this *storageServerTest) getTestObject(key, value string) (bool, error) {
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

func Run(host string, testsToRun int, done chan bool) bool {
  var key, value string
  var contains bool
  var this *storageServerTest = new(storageServerTest)
  var newKeyValues map[string]string
  
  this.host = host
  this.keyValuePairs = make(map[string]string)
  
  for ;; time.Sleep(5000 * time.Millisecond) {
    newKeyValues = make(map[string]string)
    
    fmt.Println("Generating test data...")
    // Generate random, unique key/value-pairs
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
        
        // Add both to set of new key/value-pairs, and the acutal set
        this.keyValuePairs[key] = value
        newKeyValues[key] = value
        
        runtime.Gosched()
      }
    }
    
    // Call put to insert the new key/value pairs
    for key, value = range newKeyValues {
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
      
      // Prevent exhaustion of available ports
      time.Sleep(10 * time.Millisecond)
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
      
      // Prevent exhaustion of available ports
      time.Sleep(10 * time.Millisecond)
    }
  }
  
  return true
}
