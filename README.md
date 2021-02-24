# MiniQueue

![Tests](https://github.com/tomarrell/miniqueue/workflows/Tests/badge.svg)
[![Go Report Card](https://goreportcard.com/badge/github.com/tomarrell/miniqueue)](https://goreportcard.com/report/github.com/tomarrell/miniqueue)
![GitHub release (latest by date)](https://img.shields.io/github/v/release/tomarrell/miniqueue)

A stupid simple, single binary message queue using HTTP/2.

Most messaging workloads don't require enormous amounts of data, endless
features or infinite scaling. Instead, they'd probably be better off with
something dead simple.

MiniQueue is just that. A ridiculously simple, high performance queue. You can
publish bytes to topics and be sure that your consumers will receive what you
published, nothing more.

## Features

- Simple to run
- Very fast
- Not infinitely scalable
- Multiple topics
- HTTP/2
- Publish
- Subscribe
- Acknowledgements
- Persistent
- Prometheus metrics

## API

- POST `/publish/:topic`, where the body contains the bytes to publish to the topic.

  ```bash
  curl -X POST https://localhost:8080/publish/foo --data "helloworld"
  ```

- POST `/subscribe/:topic` - streams messages separated by `\n`

  - `client → server: "INIT"`
  - `server → client: { "msg": "...", "error": "..." }`
  - `client → server: "ACK"`

You can also find example usage in the `./examples/` directory.

## Usage

MiniQueue runs as a single binary, persisting the messages to the filesystem in
a directory specified by the `-db` flag and exposes an HTTP/2 server on the port
specified by the `-port` flag.

**Note:** As the server uses HTTP/2, TLS is required. For testing, you can
generate a certificate using [mkcert](https://github.com/FiloSottile/mkcert) and
replace the ones in `./testdata` as these will not be trusted by your client, or
specify your own certificate using the `-cert` and `-key` flags.

```bash
Usage of ./miniqueue:
  -cert string
        path to TLS certificate (default "./testdata/localhost.pem")
  -db string
        path to the db file (default "./miniqueue")
  -human
        human readable logging output
  -key string
        path to TLS key (default "./testdata/localhost-key.pem")
  -level string
        (disabled|debug|info) (default "debug")
  -period duration
        period between runs to check and restore delayed messages (default 1s)
  -port int
        port used to run the server (default 8080)

```

Once running, MiniQueue will expose an HTTP/2 server capable of bidirectional
streaming between client and server. Subscribers will be delivered incoming
messages and can send commands `ACK`, `NACK`, `BACK` [etc](#commands). Upon a
subscriber disconnecting, any outstanding messages are automatically `NACK`'ed
and returned to the front of the queue.

##### Start miniqueue with human readable logs

```bash
λ ./miniqueue -human
```

##### Start miniqueue with custom TLS certificate

```bash
λ ./miniqueue -cert ./localhost.pem -key ./localhost-key.pem
```

##### Start miniqueue on custom port

```bash
λ ./miniqueue -port 8081
```

## Commands

A client may send commands to the server over a duplex connection. Commands are
in the form of a **JSON string** to allow for simple encoding/decoding.

Available commands are:

- `"INIT"`: Establishes a new consumer on the topic. If you are consuming for
    the first time, this should be sent along with the request.

- `"ACK"`: Acknowledges the current message, popping it from the topic and
    removing it.

- `"NACK"`: Negatively acknowledges the current message, causing it to be
    returned to the *front* of the queue. If there is a ready consumer waiting
    for a message, it will immediately be delivered to this consumer. Otherwise
    it will be delivered as as one becomes available.

- `"BACK"`: Negatively acknowledges the current message, causing it to be
    returned to the *back* of the queue. This will cause it to be processed
    again after the currently waiting messages.

- `"DACK [seconds]"`: Negatively acknowledges the current message, placing it on
    a delay for a certain number of `seconds`. Once the delay expires, on the
    next tick given by the `-period` flag, the message will be returned to the
    front of the queue to be processed as soon as possible.

## Benchmarks

As MiniQueue is still under development, take these benchmarks with a grain of
salt. However, for those curious:

**Publish**
```bash
λ go-wrk -c 12 -d 10 -M POST -body "helloworld" https://localhost:8080/publish/test
Running 10s test @ https://localhost:8080/publish/test
  12 goroutine(s) running concurrently
142665 requests in 9.919498387s, 7.89MB read
Requests/sec:           14382.28
Transfer/sec:           814.62KB
Avg Req Time:           834.36µs
Fastest Request:        190µs
Slowest Request:        141.091118ms
Number of Errors:       0
```

**Consume + Ack**
```bash
λ ./bench_consume -duration=10s
consumed 42982 times in 10s
4298 (consume+ack)/second
```

Running on my MacBook Pro (15-inch, 2019), with a 2.6 GHz 6-Core Intel Core i7.

## Contributing

Contributors are more than welcome. Please feel free to open a PR to improve anything you don't like, or would like to add. No PR is too small!

## License

This project is licensed under the MIT license.
