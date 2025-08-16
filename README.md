# Amelie Driver for Go

[Amelie](https://github.com/amelielabs/amelie) Driver is is an implementation of database/sql/driver interface.

## Installing

```
go get -u github.com/oskoi/amelie-go
```

## Run [Examples](https://github.com/oskoi/amelie-go/tree/main/examples)

- basic -- basic API usage.
- async -- uses `ExecuteRaw` to asynchronously wait for results.


clone repo and run container:

```bash
$ git clone https://github.com/oskoi/amelie-go && cd amelie-go
$ docker run --platform linux/amd64 -it -v ./:/home ubuntu:25.04
```

and run example in container:

```bash
$ apt-get update && apt-get install -y git golang tzdata # choose geographic area and time zone for tzdata
$ cd /home && go run examples/basic/main.go
```
