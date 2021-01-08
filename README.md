# MiniQueue

![Tests](https://github.com/tomarrell/miniqueue/workflows/Tests/badge.svg)

A stupid simple, single binary message queue using HTTP/2 push.

Most messaging workloads don't require enormous amounts of data, endless
features or infinite scaling. Instead, they'd probably be better off with
something dead simple.

MiniQueue is just that. A simple queue. You can publish bytes to topics and your
consumers will receive what you published, nothing more.

## Features

- Simple to run
- Not infinitely scalable
- HTTP/2
- Publish
- Subscribe
- Persistent
- Prometheus metrics

## API

- POST `/publish/:topic`
- GET `/subscribe/:topic`

## Usage

Run MiniQueue where you would like.

It will then expose an HTTP/2 server used for publishing and consuming.

## Contributing

Contributors are more than welcome. Please feel free to open a PR to improve anything you don't like, or would like to add. No PR is too small!

## License

This project is licensed under the MIT license.
