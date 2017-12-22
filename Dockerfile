FROM golang:1.9-alpine

RUN apk update && apk upgrade && \
    apk add --no-cache bash git openssh redis

RUN go get github.com/PuerkitoBio/goquery github.com/go-redis/redis

COPY townworknet.go /go/src/

