package frontend

import (
  "fmt"
  "io/ioutil"
  "net/http"
)

const MAX_CONTENT_LENGHT = 1024 // Maximum length of the content of the http request (1 kilobyte)
const MAX_STORAGE_SIZE = 104857600 // Maximum total storage allowed (100 megabytes)

type StorageServerFrontendHandler interface {
  GET(key string) ([]byte, error)
  PUT(key string, value []byte) error
}

type StorageServerFrontend struct {
  size uint
  cheatMap map[string][]byte
  handler StorageServerFrontendHandler
}

func (this *StorageServerFrontend) GET(key string) ([]byte, error) {
  return this.cheatMap[key], nil
}

func (this *StorageServerFrontend) PUT(key string, value []byte) error {
  this.cheatMap[key] = value
  
  return nil
}

func (this *StorageServerFrontend) ServeHTTP(w http.ResponseWriter, req *http.Request) {
  var header http.Header = w.Header()
  var key string = req.RequestURI[1:] // Remove leading slash
  var value []byte
  var err error
  
  defer req.Body.Close()
  
  switch (req.Method) {
  case "GET":
    value, err = this.handler.GET(key)
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
    
    err = this.handler.PUT(key, value)
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

func New(handler StorageServerFrontendHandler) http.Handler {
  var server *StorageServerFrontend = new(StorageServerFrontend)
  
  server.cheatMap = make(map[string][]byte)
  server.handler = handler
  if handler == nil {
    server.handler = server
  }
  
  return server
}
