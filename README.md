# FedBOX

[![MIT Licensed](https://img.shields.io/github/license/go-ap/fedbox.svg)](https://raw.githubusercontent.com/go-ap/fedbox/master/LICENSE)
[![Build Status](https://builds.sr.ht/~mariusor/fedbox.svg)](https://builds.sr.ht/~mariusor/fedbox)
[![Test Coverage](https://img.shields.io/codecov/c/github/go-ap/fedbox.svg)](https://codecov.io/gh/go-ap/fedbox)
[![Go Report Card](https://goreportcard.com/badge/github.com/go-ap/fedbox)](https://goreportcard.com/report/github.com/go-ap/fedbox)

FedBOX is a very simple ActivityPub enabled service. Its main purpose is as a reference implementation for the other [go-ap](https://github.com/go-ap) packages.

The secondary purpose is to abstract some of the common functionality that such a service would use, such as: HTTP handlers and middlewares, storage and filtering etc.

The current iteration can persist data to [BoltDB](https://go.etcd.io/bbolt) and [Badger](https://github.com/dgraph-io/badger), but I want to also add support for PostgreSQL and filesystem based method.

## Features

### Support for C2S ActivityPub:

 * Support for content management actitivies: `Create`, `Update`, `Delete`.
 All Object types are supported, but they have no local side-effects like caching images, video and audio.
 * `Follow`, `Accept`, `Block` with actors as objects.
 * Appreciation activities: `Like`, `Dislike`.
 * Negating content management and appreciation activities using `Undo`.
 * OAuth2 authentication


### Support for S2S ActivityPub

`TODO`

## Install

See [INSTALL](./doc/INSTALL.md) file.
