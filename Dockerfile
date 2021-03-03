# Builder image
FROM golang:latest as builder

COPY . /build

WORKDIR /build
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o miniqueue . 

# Exec image
FROM alpine:latest

COPY --from=builder /build/miniqueue /miniqueue

VOLUME /var/lib/miniqueue

ENTRYPOINT ["/miniqueue"]
