module github.com/makerdao/vulcanizedb

go 1.12

require (
	github.com/ClickHouse/clickhouse-go v1.4.3 // indirect
	github.com/dave/jennifer v1.3.0
	github.com/ethereum/go-ethereum v1.9.21
	github.com/hashicorp/golang-lru v0.5.4
	github.com/hpcloud/tail v1.0.0
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/jmoiron/sqlx v1.2.0
	github.com/lib/pq v1.0.0
	github.com/mitchellh/go-homedir v1.1.0
	github.com/onsi/ginkgo v1.14.0
	github.com/onsi/gomega v1.10.1
	github.com/pressly/goose v2.7.0-rc5+incompatible
	github.com/sirupsen/logrus v1.2.0
	github.com/spf13/cobra v0.0.3
	github.com/spf13/viper v1.3.2
	golang.org/x/net v0.0.0-20200822124328-c89045814202
	google.golang.org/appengine v1.6.6 // indirect
	gopkg.in/tomb.v1 v1.0.0-20141024135613-dd632973f1e7
)

replace github.com/ethereum/go-ethereum => github.com/makerdao/go-ethereum v1.9.21-rc1

replace gopkg.in/urfave/cli.v1 => gopkg.in/urfave/cli.v1 v1.20.0
