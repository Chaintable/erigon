package tracer

import (
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	dtypes "github.com/erigontech/erigon/debank/types"
	"github.com/erigontech/erigon/debank/util"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/holiman/uint256"
)

func BuildPipelineBlock(rawBlock *types.Block) dtypes.Block {
	block := dtypes.Block{
		ID:                    rawBlock.Hash().Hex(),
		Height:                rawBlock.Number(),
		ParentID:              rawBlock.ParentHash().Hex(),
		BaseFeePerGas:         big.NewInt(0),
		Miner:                 strings.ToLower(rawBlock.Coinbase().Hex()),
		GasLimit:              big.NewInt(int64(rawBlock.GasLimit())),
		GasUsed:               big.NewInt(int64(rawBlock.GasUsed())),
		Timestamp:             rawBlock.Time(),
		ProcessStartTimestamp: time.Now().UnixMilli(),
	}
	if rawBlock.Header().BaseFee != nil {
		block.BaseFeePerGas = rawBlock.Header().BaseFee
	}
	return block
}

func BuildPipelineWithdrawals(rawBlock *types.Block) []dtypes.SpecialTransfer {
	res := make([]dtypes.SpecialTransfer, 0)
	for _, withdrawal := range rawBlock.Withdrawals() {
		specialTransfer := dtypes.SpecialTransfer{
			FromAddress: strings.ToLower("0x00000000219ab540356cBB839Cbe05303d7705Fa"), //eth2 合约
			ToAddress:   strings.ToLower(withdrawal.Address.Hex()),
			Value:       (*hexutil.Big)(big.NewInt(int64(withdrawal.Amount))),
			Memo:        "beacon_withdrawl",
			Idx:         big.NewInt(int64(withdrawal.Index)),
		}
		specialTransfer.ID = util.ToHash([]string{rawBlock.Hash().Hex(), specialTransfer.ToAddress, fmt.Sprintf("%d", withdrawal.Index)})
		res = append(res, specialTransfer)
	}

	return res
}

func BuildPilelineBlockHeader(block *types.Block) *dtypes.Header {
	blockHeader := dtypes.Header{
		Number:           (*hexutil.Big)(block.Number()),
		Hash:             block.Hash(),
		ParentHash:       block.ParentHash(),
		Nonce:            block.Nonce(),
		MixHash:          block.MixDigest(),
		Sha3Uncles:       block.UncleHash(),
		LogsBloom:        block.Bloom(),
		StateRoot:        block.Root(),
		Miner:            block.Coinbase(),
		Difficulty:       (*hexutil.Big)(block.Difficulty()),
		ExtraData:        hexutil.Bytes(block.Extra()),
		GasLimit:         hexutil.Uint64(block.GasLimit()),
		GasUsed:          hexutil.Uint64(block.GasUsed()),
		Timestamp:        hexutil.Uint64(block.Time()),
		TransactionsRoot: block.TxHash(),
		ReceiptsRoot:     block.ReceiptHash(),
	}
	if block.Header().BaseFee != nil {
		blockHeader.BaseFeePerGas = (*hexutil.Big)(block.Header().BaseFee)
	}
	if block.Header().WithdrawalsHash != nil {
		blockHeader.WithdrawalsRoot = block.Header().WithdrawalsHash
	}
	if block.Header().BlobGasUsed != nil {
		blockHeader.BlobGasUsed = (*hexutil.Uint64)(block.Header().BlobGasUsed)
	}
	if block.Header().ExcessBlobGas != nil {
		blockHeader.ExcessBlobGas = (*hexutil.Uint64)(block.Header().ExcessBlobGas)
	}
	if block.Header().ParentBeaconBlockRoot != nil {
		blockHeader.ParentBeaconBlockRoot = block.Header().ParentBeaconBlockRoot
	}
	if block.Header().RequestsHash != nil {
		blockHeader.RequestsRoot = block.Header().RequestsHash
	}
	return &blockHeader
}

func BuildPipelineTransaction(tx types.Transaction, receipt *types.Receipt, from common.Address, chainConfig *chain.Config, header *types.Header) dtypes.Transaction {
	to := receipt.ContractAddress
	if tx.GetTo() != nil {
		to = *tx.GetTo()
	}
	gasPrice := big.NewInt(0)
	if !chainConfig.IsLondon(header.Number.Uint64()) {
		gasPrice = tx.GetFeeCap().ToBig()
	} else {
		baseFee, _ := uint256.FromBig(header.BaseFee)
		gasPrice = new(big.Int).Add(header.BaseFee, tx.GetEffectiveGasTip(baseFee).ToBig())
	}
	if gasPrice.Cmp(big.NewInt(0)) == 0 {
		gasPrice = tx.GetFeeCap().ToBig()
	}
	transaction := dtypes.Transaction{
		ID:               tx.Hash().Hex(),
		From:             strings.ToLower(from.Hex()),
		To:               strings.ToLower(to.Hex()),
		Gas:              big.NewInt(int64(tx.GetGasLimit())),
		GasPrice:         gasPrice,
		GasUsed:          big.NewInt(int64(receipt.GasUsed)),
		Status:           receipt.Status == types.ReceiptStatusSuccessful,
		GasFeeCap:        common.Big0,
		GasTipCap:        common.Big0,
		Input:            tx.GetData(),
		Nonce:            big.NewInt(int64(tx.GetNonce())),
		TransactionIndex: int64(receipt.TransactionIndex),
		Value:            (*hexutil.Big)(tx.GetValue().ToBig()),
	}
	switch tx.Type() {
	case types.DynamicFeeTxType, types.BlobTxType, types.SetCodeTxType:
		transaction.GasFeeCap = tx.GetFeeCap().ToBig()
		transaction.GasTipCap = tx.GetTipCap().ToBig()
	}
	return transaction
}

func BuildBorPipelineTransaction(tx types.Transaction, receipt *types.Receipt, txHash common.Hash) dtypes.Transaction {
	txn := tx.(*types.LegacyTx)
	to := receipt.ContractAddress
	if tx.GetTo() != nil {
		to = *tx.GetTo()
	}
	transaction := dtypes.Transaction{
		ID:               txHash.Hex(),
		From:             strings.ToLower(common.Address{}.Hex()),
		To:               strings.ToLower(to.Hex()),
		Gas:              big.NewInt(int64(tx.GetGasLimit())),
		GasPrice:         txn.GasPrice.ToBig(),
		GasUsed:          big.NewInt(int64(receipt.GasUsed)),
		Status:           receipt.Status == types.ReceiptStatusSuccessful,
		GasFeeCap:        common.Big0,
		GasTipCap:        common.Big0,
		Input:            tx.GetData(),
		Nonce:            big.NewInt(int64(tx.GetNonce())),
		TransactionIndex: int64(receipt.TransactionIndex),
		Value:            (*hexutil.Big)(tx.GetValue().ToBig()),
	}
	return transaction
}
