package integration_test

import (
	"errors"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/makerdao/vulcanizedb/pkg/config"
	"github.com/makerdao/vulcanizedb/pkg/core"
	"github.com/makerdao/vulcanizedb/pkg/eth"
	"github.com/makerdao/vulcanizedb/pkg/eth/client"
	"github.com/makerdao/vulcanizedb/pkg/eth/converters"
	"github.com/makerdao/vulcanizedb/pkg/eth/node"
	"github.com/makerdao/vulcanizedb/test_config"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
)

var TestClient config.Client

func init() {
	ipc := test_config.TestConfig.GetString("client.ipcPath")

	// If we don't have an ipc path in the config file, check the env variable
	if ipc == "" {
		test_config.TestConfig.BindEnv("url", "CLIENT_IPCPATH")
		ipc = test_config.TestConfig.GetString("url")
	}
	if ipc == "" {
		logrus.Fatal(errors.New("testing.toml IPC path or $CLIENT_IPCPATH env variable need to be set"))
	}

	TestClient = config.Client{
		IPCPath: ipc,
	}
}

func SetupBC() core.BlockChain {
	con := TestClient
	testIPC := con.IPCPath
	rawRPCClient, err := rpc.Dial(testIPC)
	Expect(err).NotTo(HaveOccurred())
	rpcClient := client.NewRpcClient(rawRPCClient, testIPC)
	ethClient := ethclient.NewClient(rawRPCClient)
	blockChainClient := client.NewEthClient(ethClient)
	madeNode := node.MakeNode(rpcClient)
	transactionConverter := converters.NewTransactionConverter(ethClient)
	blockChain := eth.NewBlockChain(blockChainClient, rpcClient, madeNode, transactionConverter)

	return blockChain
}
