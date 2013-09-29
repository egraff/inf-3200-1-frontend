inf-3200-1-frontend
===================

Frontend storage server for INF-3200 re-written in Go.

# Get it!
    go get github.com/egraff/inf-3200-1-frontend

# How to use it?
Implement the GET() and PUT() handlers in main.go, and uncomment the line in main() that looks like
    handler = &DHTFrontendHandler{nodes}
