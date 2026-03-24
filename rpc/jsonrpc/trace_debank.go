package jsonrpc

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/metrics"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	dtracer "github.com/erigontech/erigon/debank/tracer"
	dtypes "github.com/erigontech/erigon/debank/types"
	"github.com/erigontech/erigon/eth/consensuschain"
	"github.com/erigontech/erigon/eth/tracers"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	bortypes "github.com/erigontech/erigon/polygon/bor/types"
	polygonchain "github.com/erigontech/erigon/polygon/chain"
	"github.com/erigontech/erigon/polygon/tracer"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpchelper"
	"github.com/erigontech/erigon/turbo/transactions"
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
		genesis := polygonchain.BorMainnetGenesisBlock()
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

	stateReader, err := CreateHistoryStateReader2(dbtx, api._txNumReader, header.Number.Uint64(), 0, chainConfig.ChainName)
	if err != nil {
		return nil, err
	}

	writer := dtracer.NewBlockStorageDiff()
	ibs := state.New(stateReader)
	usedGas := new(uint64)
	usedBlobGas := new(uint64)
	gp := new(core.GasPool).AddGas(header.GasLimit).AddBlobGas(chainConfig.GetMaxBlobGasPerBlock(header.Time))

	engine, ok := api.engine().(consensus.Engine)
	if !ok {
		return nil, errors.New("engine is not consensus.Engine")
	}

	consensusHeaderReader := consensuschain.NewReader(chainConfig, dbtx, api._blockReader, nil)
	logger := log.New("trace_debankBlock")

	err = core.InitializeBlockExecution(engine, consensusHeaderReader, block.HeaderNoCopy(), chainConfig, ibs, writer, logger, nil)
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
			return nil, e
		}
		return h, nil
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

	for i, txn := range block.Transactions() {
		ibs.SetTxContext(blockNumber, i)
		tracer := dtracer.NewCallTracer(blockFile, txn.Hash().Hex())
		vmConfig.Tracer = tracer
		ibs.SetHooks(&tracing.Hooks{
			OnLog: tracer.OnLog,
		})
		receipt, _, err := core.ApplyTransaction(chainConfig, core.GetHashFn(header, getHeader), engine, nil, gp, ibs, writer, header, txn, usedGas, usedBlobGas, vmConfig)
		if err != nil {
			return nil, fmt.Errorf("trace_debankBlock: bn=%d, txnIdx=%d, %w", header.Number.Uint64(), i, err)
		}
		includedTxs = append(includedTxs, txn)
		receipts = append(receipts, receipt)
		tx := dtracer.BuildPipelineTransaction(txn, receipt, getFrom(txn), chainConfig, header)
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
			stateReceiverContract := chainConfig.Bor.(*borcfg.BorConfig).StateReceiverContractAddress()
			txCtx := evmtypes.TxContext{
				TxHash:   borTxHash,
				Origin:   common.Address{},
				GasPrice: uint256.NewInt(0),
			}
			atracer := dtracer.NewCallTracer(blockFile, txCtx.TxHash.Hex())
			vmConfig.Tracer = atracer
			if vmConfig.Tracer != nil {
				vmConfig.Tracer = tracer.NewBorStateSyncTxnTracer(&tracers.Tracer{
					Hooks:     vmConfig.Tracer,
					GetResult: nil,
					Stop:      nil,
				}, stateReceiverContract).Hooks
			}
			ibs.SetTxContext(blockNumber, len(block.Transactions()))
			ibs.SetHooks(&tracing.Hooks{
				OnLog: atracer.OnLog,
			})
			blockCtx := transactions.NewEVMBlockContext(engine, header, true, dbtx, api._blockReader, chainConfig)
			evm := vm.NewEVM(blockCtx, txCtx, ibs, chainConfig, vmConfig)
			rules := blockCtx.Rules(chainConfig)

			// OnTxStart initializes the callTracer's callstack and txID.
			if vmConfig.Tracer != nil && vmConfig.Tracer.OnTxStart != nil {
				vmConfig.Tracer.OnTxStart(evm.GetVMContext(), bortypes.NewBorTransaction(), common.Address{})
			}

			for _, msg := range stateSyncEvents {
				gp := new(core.GasPool).AddGas(msg.Gas()).AddBlobGas(msg.BlobGas())
				_, err = core.ApplyMessage(evm, msg, gp, true, false /* gasBailout */, api.engine())
				if err != nil {
					return nil, err
				}

				err = ibs.FinalizeTx(rules, writer)
				if err != nil {
					return nil, err
				}

				evm.Reset(txCtx, ibs)
			}

			receipt := &types.Receipt{
				Type:             0,
				TxHash:           borTxHash,
				GasUsed:          0,
				BlockHash:        block.Hash(),
				BlockNumber:      block.Number(),
				TransactionIndex: uint(len(block.Transactions())),
				Status:           types.ReceiptStatusSuccessful,
			}

			if vmConfig.Tracer != nil && vmConfig.Tracer.OnTxEnd != nil {
				vmConfig.Tracer.OnTxEnd(receipt, nil)
			}

			tx := dtracer.BuildBorPipelineTransaction(borTx, receipt, borTxHash)
			blockFile.Txs = append(blockFile.Txs, tx)
		}

	}

	blockCtx := core.NewEVMBlockContext(header, core.GetHashFn(header, nil), engine, nil, chainConfig)
	if err := ibs.CommitBlock(blockCtx.Rules(chainConfig), writer); err != nil {
		return nil, fmt.Errorf("committing block %d failed: %w", header.Number.Uint64(), err)
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

	stateDiff := writer.ToStateDiff(parentHeader.Root, block.Root())

	for addr := range writer.StorageChanges {
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

func CreateHistoryStateReader2(tx kv.TemporalTx, txNumsReader rawdbv3.TxNumsReader, blockNumber uint64, txnIndex int, chainName string) (state.StateReader, error) {
	r := state.NewHistoryReaderV3()
	r.SetTx(tx)
	//r.SetTrace(true)
	minTxNum, err := txNumsReader.Min(tx, blockNumber)
	if err != nil {
		return nil, err
	}
	txNum := uint64(int(minTxNum) + txnIndex)
	earliestTxNum := r.StateHistoryStartFrom()
	if txNum < earliestTxNum {
		// data available only starting from earliestTxNum, throw error to avoid unintended
		// consequences of using this StateReader
		return r, state.PrunedError
	}
	r.SetTxNum(txNum)
	return r, nil
}

func getFrom(txn types.Transaction) common.Address {
	var chainId *big.Int
	switch t := txn.(type) {
	case *types.LegacyTx:
		if t.Protected() {
			chainId = types.DeriveChainId(&t.V).ToBig()
		}
	default:
		chainId = txn.GetChainID().ToBig()
	}

	var from common.Address
	signer := types.LatestSignerForChainID(chainId)
	from, _ = txn.Sender(*signer)
	return from
}
