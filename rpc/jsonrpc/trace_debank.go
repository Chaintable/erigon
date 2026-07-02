package jsonrpc

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/consensuschain"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	dtracer "github.com/erigontech/erigon/debank/tracer"
	dtypes "github.com/erigontech/erigon/debank/types"
	"github.com/erigontech/erigon/diagnostics/metrics"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	bortypes "github.com/erigontech/erigon/polygon/bor/types"
	polygonchain "github.com/erigontech/erigon/polygon/chain"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpchelper"
	"github.com/erigontech/erigon/rpc/transactions"
	"github.com/holiman/uint256"
)

var (
	LatestBlockNumber = metrics.GetOrCreateGauge("pipeline_block_num")

	ChainHeadNumber = metrics.GetOrCreateGauge("chain_head_block")

	LatestBlockTime = metrics.GetOrCreateGauge("pipeline_block_time")

	NodeInfo = metrics.GetOrCreateGauge(`pipeline_node_info{role="writer"}`)

	BlockProcessTimer = metrics.GetOrCreateSummary("chain_inserts")
)

func (api *TraceAPIImpl) DebankBlockRaw(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (*dtypes.DebankOutPut, error) {
	dbtx, err := api.kv.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer dbtx.Rollback()

	chainConfig, err := api.chainConfig(ctx, dbtx)
	if err != nil {
		return nil, err
	}

	blockNumber, blockHash, _, err := rpchelper.GetBlockNumber(ctx, blockNrOrHash, dbtx, api._blockReader, api.filters)
	if err != nil {
		return nil, err
	}

	// Extract transactions from block
	block, bErr := api.blockWithSenders(ctx, dbtx, blockHash, blockNumber)
	if bErr != nil {
		return nil, bErr
	}

	if block == nil {
		return nil, fmt.Errorf("could not find block  %d", blockNumber)
	}

	header := block.Header()

	if block.NumberU64() == 0 {
		genesis := genesisBlockByChainName(chainConfig.ChainName)
		return dtracer.OnGenesisBlock(block, genesis.Alloc)
	}

	LatestBlockNumber.SetUint64(block.NumberU64())
	ChainHeadNumber.SetUint64(block.NumberU64())
	LatestBlockTime.SetUint64(block.Time())

	parentHash := block.ParentHash()
	parentHeader, err := api._blockReader.Header(ctx, dbtx, parentHash, block.NumberU64()-1)
	if err != nil {
		return nil, err
	}

	stateReader, err := CreateHistoryStateReader2(ctx, dbtx, api._txNumReader, header.Number.Uint64(), 0, chainConfig.ChainName)
	if err != nil {
		return nil, err
	}

	writer := dtracer.NewBlockStorageDiff()
	ibs := state.New(stateReader)
	gasUsed := &protocol.GasUsed{}
	gp := protocol.NewGasPool(header.GasLimit, chainConfig.GetMaxBlobGasPerBlock(header.Time))

	engine, ok := api.engine().(rules.Engine)
	if !ok {
		return nil, errors.New("engine is not rules.Engine")
	}

	consensusHeaderReader := consensuschain.NewReader(chainConfig, dbtx, api._blockReader, nil)
	logger := log.New("trace_debankBlock")

	err = protocol.InitializeBlockExecution(engine, consensusHeaderReader, block.HeaderNoCopy(), chainConfig, ibs, writer, logger, nil)
	if err != nil {
		return nil, err
	}

	includedTxs := make(types.Transactions, 0, block.Transactions().Len())
	receipts := make(types.Receipts, 0, block.Transactions().Len())
	vmConfig := vm.Config{}

	getHeader := func(hash common.Hash, number uint64) (*types.Header, error) {
		h, e := api._blockReader.Header(ctx, dbtx, hash, number)
		if e != nil {
			log.Error("getHeader error", "number", number, "hash", hash, "err", e)
		}
		return h, e
	}
	blockFile := &dtypes.BlockFile{
		Block:            dtracer.BuildPipelineBlock(block),
		Events:           make([]dtypes.Event, 0),
		Txs:              make([]dtypes.Transaction, 0),
		Traces:           make([]dtypes.Trace, 0),
		ErrorEvents:      make([]dtypes.Event, 0),
		ErrorTraces:      make([]dtypes.Trace, 0),
		StorageContracts: make([]string, 0),
	}
	stateHeader := dtracer.BuildPilelineBlockHeader(block)

	// Collect ChangeContracts from all tracers to deduplicate them
	changeContractsMap := make(map[common.Address]struct{})
	for i, txn := range block.Transactions() {
		ibs.SetTxContext(header.Number.Uint64(), i)
		hooks, tracer := dtracer.GetCallTracer(blockFile, txn.Hash().Hex())
		vmConfig.Tracer = hooks
		ibs.SetHooks(hooks)
		receipt, err := protocol.ApplyTransaction(chainConfig, protocol.GetHashFn(header, getHeader), engine, accounts.NilAddress, gp, ibs, writer, header, txn, gasUsed, vmConfig)
		if err != nil {
			return nil, fmt.Errorf("trace_debankBlock: bn=%d, txnIdx=%d, %w", header.Number.Uint64(), i, err)
		}
		for addr := range tracer.ChangeContracts {
			changeContractsMap[addr] = struct{}{}
		}
		includedTxs = append(includedTxs, txn)
		receipts = append(receipts, receipt)
		from := getFrom(txn)
		tx := dtracer.BuildPipelineTransaction(txn, receipt, from, chainConfig, header)
		blockFile.Txs = append(blockFile.Txs, tx)
	}

	if chainConfig.Bor != nil {
		var borTx types.Transaction
		var borTxHash common.Hash
		possibleBorTxnHash := bortypes.ComputeBorTxHash(block.NumberU64(), block.Hash())
		_, ok, err := api.bridgeReader.EventTxnLookup(ctx, possibleBorTxnHash)
		if err != nil {
			return nil, err
		}
		if ok {
			borTx = bortypes.NewBorTransaction()
			borTxHash = possibleBorTxnHash
		}
		if borTx != nil {
			var stateSyncEvents []*types.Message
			stateSyncEvents, err = api.bridgeReader.Events(ctx, header.Hash(), blockNumber)
			if err != nil {
				return nil, err
			}
			txCtx := evmtypes.TxContext{
				TxHash:   bortypes.ComputeBorTxHash(blockNumber, blockHash),
				Origin:   accounts.ZeroAddress,
				GasPrice: *uint256.NewInt(0),
			}
			hooks := dtracer.NewCallTracer(blockFile, txCtx.TxHash.Hex())
			vmConfig.Tracer = hooks
			ibs.SetTxContext(header.Number.Uint64(), len(block.Transactions()))
			ibs.SetHooks(hooks)
			blockCtx := transactions.NewEVMBlockContext(engine, header, true, dbtx, api._blockReader, chainConfig)
			evm := vm.NewEVM(blockCtx, txCtx, ibs, chainConfig, vmConfig)
			rules := blockCtx.Rules(chainConfig)
			for _, msg := range stateSyncEvents {
				gp := protocol.NewGasPool(msg.Gas(), msg.BlobGas())
				_, err := protocol.ApplyMessage(evm, msg, gp, true, false /* gasBailout */, engine)
				if err != nil {
					return nil, err
				}

				err = ibs.FinalizeTx(rules, writer)
				if err != nil {
					return nil, err
				}

				evm.Reset(txCtx, ibs)
			}

			blockNum := block.Number()
			receipt := types.Receipt{
				Type:             0,
				TxHash:           bortypes.ComputeBorTxHash(block.NumberU64(), block.Hash()),
				GasUsed:          0,
				BlockHash:        block.Hash(),
				BlockNumber:      &blockNum,
				TransactionIndex: uint(len(block.Transactions())),
				Status:           types.ReceiptStatusSuccessful,
			}

			tx := dtracer.BuildBorPipelineTransaction(borTx, &receipt, borTxHash)
			blockFile.Txs = append(blockFile.Txs, tx)
		}

	}

	chainReader := consensuschain.NewReader(chainConfig, dbtx, api._blockReader, logger)

	newBlock, _, err := protocol.FinalizeBlockExecution(engine, stateReader, block.Header(), block.Transactions(), block.Uncles(), writer, chainConfig, ibs, receipts, block.Withdrawals(), chainReader, true, logger, nil)
	if err != nil {
		return nil, err
	}

	if newBlock.Root() != block.Root() {
		return nil, fmt.Errorf("state root mismatch")
	}

	receiptSha := types.DeriveSha(receipts)
	if chainConfig.IsByzantium(header.Number.Uint64()) && receiptSha != block.ReceiptHash() {
		return nil, fmt.Errorf("receipt hash mismatch")
	}

	txSha := types.DeriveSha(includedTxs)
	if txSha != block.TxHash() {
		return nil, fmt.Errorf("tx hash mismatch")
	}

	// 如果 usedGas 不为 nil，值必须等于 headerGasUsed
	if gasUsed.BlockGasUsed() != header.GasUsed {
		return nil, fmt.Errorf("usedGas mismatch: got %v, want %v", gasUsed.BlockGasUsed(), header.GasUsed)
	}

	// usedBlobGas 不为 nil
	if header.BlobGasUsed == nil {
		// 将 headerBlobGasUsed 视为 0
		if gasUsed.Blob != 0 {
			return nil, fmt.Errorf("usedBlobGas is %v, but headerBlobGasUsed is nil (0 expected)", gasUsed.Blob)
		}
	} else {
		// headerBlobGasUsed 不为 nil，二者必须相等
		if gasUsed.Blob != *header.BlobGasUsed {
			return nil, fmt.Errorf("usedBlobGas mismatch: got %v, want %v", gasUsed.Blob, *header.BlobGasUsed)
		}
	}

	bloom := types.CreateBloom(receipts)
	if bloom != header.Bloom {
		return nil, fmt.Errorf("bloom mismatch")
	}

	stateDiff := writer.ToStateDiff(parentHeader.Root, newBlock.Root())

	for addr := range changeContractsMap {
		blockFile.StorageContracts = append(blockFile.StorageContracts, strings.ToLower(addr.Hex()))
	}

	out := &dtypes.DebankOutPut{
		BlockFile:      blockFile,
		Header:         stateHeader,
		StateDiff:      stateDiff,
		ValidationHash: blockFile.Validation().ValidationHash,
	}

	return out, nil
}

type DebankOutPutJs struct {
	BlockFile      *dtypes.BlockFile `json:"block_file"`
	Header         *dtypes.Header    `json:"header"`
	StateDiff      hexutil.Bytes     `json:"state_diff"`
	ValidationHash int64             `json:"validation_hash"`
}

func (api *TraceAPIImpl) DebankBlock(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (*DebankOutPutJs, error) {
	start := time.Now()
	output, err := api.DebankBlockRaw(ctx, blockNrOrHash)
	if err != nil {
		return nil, err
	}
	data, err := rlp.EncodeToBytes(output.StateDiff)
	if err != nil {
		return nil, err
	}

	BlockProcessTimer.Observe(float64(time.Since(start)))
	return &DebankOutPutJs{
		BlockFile:      output.BlockFile,
		Header:         output.Header,
		StateDiff:      data,
		ValidationHash: output.ValidationHash,
	}, nil
}

func (api *TraceAPIImpl) DebankBGTraceStart(ctx context.Context, region string, nodeXBucket string, chainTableBucket string, broker string, topic string, chainID string, version string, startBlock, endBlock, maxTask uint64) (*BGTraceStatus, error) {
	if region == "" || nodeXBucket == "" || chainTableBucket == "" || broker == "" || topic == "" || chainID == "" || endBlock == 0 || startBlock > endBlock || maxTask == 0 {
		return nil, errors.New("missing required parameters")
	}

	err := dtracer.DebankTraceBackGroundMangeInstance.Start(api, region, nodeXBucket, chainTableBucket, broker, topic, chainID, version, startBlock, endBlock, maxTask)
	if err != nil {
		return nil, err
	}
	stat, err := api.DebankBGTraceStatus(ctx)
	if err != nil {
		return nil, err
	}

	return stat, nil
}

func (api *TraceAPIImpl) DebankBGTraceStop(ctx context.Context) (*BGTraceStatus, error) {
	stat, err := api.DebankBGTraceStatus(ctx)
	if err != nil {
		return nil, err
	}
	dtracer.DebankTraceBackGroundMangeInstance.Stop()
	return stat, nil
}

type BGTraceStatus struct {
	Start     uint64  `json:"start"`
	End       uint64  `json:"end"`
	Latest    uint64  `json:"latest"`
	Blocks    uint64  `json:"blocks"`
	StartTime uint64  `json:"start_time"`
	Duration  uint64  `json:"duration"`
	Rate      float64 `json:"rate"`
}

func (api *TraceAPIImpl) DebankBGTraceStatus(ctx context.Context) (*BGTraceStatus, error) {
	start, end, latest, startTime := dtracer.DebankTraceBackGroundMangeInstance.Status()
	return &BGTraceStatus{
		Start:     start,
		End:       end,
		Latest:    latest,
		Blocks:    latest - start + 1,
		StartTime: uint64(startTime.Unix()),
		Duration:  uint64(time.Now().Unix() - startTime.Unix()),
		Rate:      float64(latest-start+1) / float64(time.Now().Unix()-startTime.Unix()),
	}, nil

}

func CreateHistoryStateReader2(ctx context.Context, tx kv.TemporalTx, txNumsReader rawdbv3.TxNumsReader, blockNumber uint64, txnIndex int, chainName string) (state.StateReader, error) {
	minTxNum, err := txNumsReader.Min(ctx, tx, blockNumber)
	if err != nil {
		return nil, err
	}
	txNum := uint64(int(minTxNum) + txnIndex + /* 1 system txNum in beginning of block */ 1)
	if minHistoryTxNum := state.StateHistoryStartTxNum(tx); txNum < minHistoryTxNum {
		firstAvailBlock, _, _ := txNumsReader.FindBlockNum(ctx, tx, minHistoryTxNum)
		return nil, fmt.Errorf("%w: requested block %d, history is available from block %d", state.PrunedError, blockNumber, firstAvailBlock)
	}
	return state.NewHistoryReaderV3(tx, txNum), nil
}

func getFrom(txn types.Transaction) common.Address {
	var chainId *uint256.Int
	switch t := txn.(type) {
	case *types.LegacyTx:
		if t.Protected() {
			chainId = types.DeriveChainId(&t.V)
		}
	default:
		chainId = txn.GetChainID()
	}

	var from accounts.Address
	signer := types.LatestSignerForChainID(chainId)
	from, _ = txn.Sender(*signer)
	return from.Value()
}

// genesisBlockByChainName returns the genesis block for the given chain name
func genesisBlockByChainName(chainName string) *types.Genesis {
	switch chainName {
	case "mainnet":
		return chainspec.MainnetGenesisBlock()
	case "sepolia":
		return chainspec.SepoliaGenesisBlock()
	case "hoodi":
		return chainspec.HoodiGenesisBlock()
	case "gnosis":
		return chainspec.GnosisGenesisBlock()
	case "chiado":
		return chainspec.ChiadoGenesisBlock()
	case "bor-mainnet":
		return polygonchain.BorMainnetGenesisBlock()
	case "mumbai":
		return polygonchain.MumbaiGenesisBlock()
	case "amoy":
		return polygonchain.AmoyGenesisBlock()
	case "bor-devnet":
		return polygonchain.BorDevnetGenesisBlock()
	default:
		return nil
	}
}
