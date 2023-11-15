#!/bin/bash

# mac
#CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -o s3tools main.go

# linux
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o weed_nebulas weed/weed.go
