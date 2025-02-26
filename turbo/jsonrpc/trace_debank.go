package jsonrpc

import (
	"context"
	"errors"
	"fmt"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm"
	dtracer "github.com/erigontech/erigon/debank/tracer"
	dtypes "github.com/erigontech/erigon/debank/types"
	"github.com/erigontech/erigon/eth/consensuschain"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/turbo/rpchelper"
	"github.com/erigontech/erigon/turbo/shards"
)

type DebankOutPut struct {
	BlockFile      *dtypes.BlockFile `json:"block_file"`
	Header         *dtypes.Header    `json:"header"`
	StateDiff      []byte            `json:"state_diff"`
	ValidationHash int64             `json:"validation_hash"`
}

func (api *TraceAPIImpl) DebankBlock(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (*DebankOutPut, error) {
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
		return nil, fmt.Errorf("cannot trace genesis block")
	}

	parentNo := rpc.BlockNumber(block.NumberU64() - 1)
	parentHash := block.ParentHash()
	parentNrOrHash := rpc.BlockNumberOrHash{
		BlockNumber:      &parentNo,
		BlockHash:        &parentHash,
		RequireCanonical: true,
	}

	parentHeader, err := api._blockReader.Header(ctx, dbtx, parentHash, block.NumberU64()-1)
	if err != nil {
		return nil, err
	}

	stateReader, err := rpchelper.CreateStateReader(ctx, dbtx, api._blockReader, parentNrOrHash, 0, api.filters, api.stateCache, chainConfig.ChainName)
	if err != nil {
		return nil, err
	}
	stateCache := shards.NewStateCache(32, 0)
	cachedReader := state.NewCachedReader(stateReader, stateCache)
	writer := dtracer.NewBlockStorageDiff()
	cachedWriter := state.NewCachedWriter(writer, stateCache)
	ibs := state.New(cachedReader)
	usedGas := new(uint64)
	usedBlobGas := new(uint64)
	gp := new(core.GasPool).AddGas(header.GasLimit).AddBlobGas(chainConfig.GetMaxBlobGasPerBlock(header.Time))

	engine, ok := api.engine().(consensus.Engine)
	if !ok {
		return nil, errors.New("engine is not consensus.Engine")
	}

	consensusHeaderReader := consensuschain.NewReader(chainConfig, dbtx, api._blockReader, nil)
	logger := log.New("trace_debankBlock")

	err = core.InitializeBlockExecution(engine, consensusHeaderReader, block.HeaderNoCopy(), chainConfig, ibs, cachedWriter, logger, nil)
	if err != nil {
		return nil, err
	}

	includedTxs := make(types.Transactions, 0, block.Transactions().Len())
	receipts := make(types.Receipts, 0, block.Transactions().Len())
	vmConfig := vm.Config{}

	getHeader := func(hash common.Hash, number uint64) *types.Header {
		h, e := api._blockReader.Header(ctx, dbtx, hash, number)
		if e != nil {
			log.Error("getHeader error", "number", number, "hash", hash, "err", e)
		}
		return h
	}
	blockFile := &dtypes.BlockFile{
		Block:            dtracer.BuildPipelineBlock(block),
		SpecialTransfers: dtracer.BuildPipelineWithdrawals(block),
		Events:           make([]dtypes.Event, 0),
		Txs:              make([]dtypes.Transaction, 0),
		Traces:           make([]dtypes.Trace, 0),
	}
	stateHeader := dtracer.BuildPilelineBlockHeader(block)
	balanceTracer := &dtracer.BalanceTracer{BlockFile: blockFile}
	// 为empty block 设置 balanceTracer
	ibs.SetHooks(&tracing.Hooks{
		OnBalanceChange: balanceTracer.OnBalanceChange,
	})
	for i, txn := range block.Transactions() {
		ibs.SetTxContext(i)
		tracer := dtracer.NewCallTracer(blockFile, txn.Hash().Hex())
		vmConfig.Debug = true
		vmConfig.Tracer = tracer
		ibs.SetHooks(&tracing.Hooks{
			OnLog:           tracer.OnLog,
			OnBalanceChange: balanceTracer.OnBalanceChange,
		})
		receipt, _, err := core.ApplyTransaction(chainConfig, core.GetHashFn(header, getHeader), engine, nil, gp, ibs, cachedWriter, header, txn, usedGas, usedBlobGas, vmConfig)
		if err != nil {
			return nil, fmt.Errorf("trace_debankBlock: bn=%d, txnIdx=%d, %w", header.Number.Uint64(), i, err)
		}
		includedTxs = append(includedTxs, txn)
		receipts = append(receipts, receipt)
		from := tracer.Evm.Origin
		tx := dtracer.BuildPipelineTransaction(txn, receipt, from, chainConfig, header)
		blockFile.Txs = append(blockFile.Txs, tx)
	}

	chainReader := consensuschain.NewReader(chainConfig, dbtx, api._blockReader, logger)

	newBlock, _, _, _, err := core.FinalizeBlockExecution(engine, stateReader, block.Header(), block.Transactions(), block.Uncles(), cachedWriter, chainConfig, ibs, receipts, block.Withdrawals(), chainReader, true, logger)
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
	if *usedGas != header.GasUsed {
		return nil, fmt.Errorf("usedGas mismatch: got %v, want %v", *usedGas, header.GasUsed)
	}

	// usedBlobGas 不为 nil
	if header.BlobGasUsed == nil {
		// 将 headerBlobGasUsed 视为 0
		if *usedBlobGas != 0 {
			return nil, fmt.Errorf("usedBlobGas is %v, but headerBlobGasUsed is nil (0 expected)", *usedBlobGas)
		}
	} else {
		// headerBlobGasUsed 不为 nil，二者必须相等
		if *usedBlobGas != *header.BlobGasUsed {
			return nil, fmt.Errorf("usedBlobGas mismatch: got %v, want %v", *usedBlobGas, *header.BlobGasUsed)
		}
	}

	bloom := types.CreateBloom(receipts)
	if bloom != header.Bloom {
		return nil, fmt.Errorf("bloom mismatch")
	}

	stateDiff := writer.ToStateDiff(parentHeader.Root, newBlock.Root())

	data, err := rlp.EncodeToBytes(stateDiff)
	if err != nil {
		return nil, err
	}

	out := &DebankOutPut{
		BlockFile:      blockFile,
		Header:         stateHeader,
		StateDiff:      data,
		ValidationHash: blockFile.Validation().ValidationHash,
	}

	return out, nil
}
