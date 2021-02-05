# MiniQueue

![Tests](https://github.com/tomarrell/miniqueue/workflows/Tests/badge.svg)
<img src="https://img.shields.io/badge/status-WIP-yellow">

A stupid simple, single binary message queue using HTTP/2.

Most messaging workloads don't require enormous amounts of data, endless
features or infinite scaling. Instead, they'd probably be better off with
something dead simple.

MiniQueue is just that. A simple queue. You can publish bytes to topics and your
consumers will receive what you published, nothing more.

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

- POST `/publish/:topic`
- GET `/subscribe/:topic`

## Usage

Run MiniQueue where you would like.

It will then expose an HTTP/2 server used for publishing and consuming.

## Benchmarks

As MiniQueue is under heavy development, take these benchmarks with a grain of
salt. However, for those curious:

```
λ ~/ go-wrk -c 12 -d 10 -M POST -body "helloworld" https://localhost:8080/publish/test
Running 10s test @ https://localhost:8080/publish/test
  12 goroutine(s) running concurrently
104084 requests in 9.942585489s, 5.76MB read
Requests/sec:           10468.50
Transfer/sec:           592.94KB
Avg Req Time:           1.146295ms
Fastest Request:        262.281µs
Slowest Request:        867.958415ms
Number of Errors:       0
```

Running on my MacBook Pro (15-inch, 2019), with a 2.6 GHz 6-Core Intel Core i7.

## Contributing

Contributors are more than welcome. Please feel free to open a PR to improve anything you don't like, or would like to add. No PR is too small!

## License

This project is licensed under the MIT license.
