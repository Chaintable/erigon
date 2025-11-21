package tracer

import (
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/types/accounts"
	"github.com/erigontech/erigon/accounts/abi"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm"
	dtypes "github.com/erigontech/erigon/debank/types"
	"github.com/erigontech/erigon/debank/util"
	"github.com/holiman/uint256"
)

type BlockStorageDiffMap struct {
	NewAccounts     map[common.Hash]dtypes.NewAccount
	DeletedAccounts map[common.Hash]struct{}
	StorageDiff     map[common.Hash]map[common.Hash]*uint256.Int
	NewCodes        map[common.Hash]dtypes.NewCode
	StorageChanges  map[common.Address]struct{} // Used to track storage changes for contracts
}

func NewBlockStorageDiff() *BlockStorageDiffMap {
	return &BlockStorageDiffMap{
		NewAccounts:     make(map[common.Hash]dtypes.NewAccount),
		DeletedAccounts: make(map[common.Hash]struct{}),
		StorageDiff:     make(map[common.Hash]map[common.Hash]*uint256.Int),
		NewCodes:        make(map[common.Hash]dtypes.NewCode),
		StorageChanges:  make(map[common.Address]struct{}),
	}
}

func (bs *BlockStorageDiffMap) ToStateDiff(parrentRoot, root common.Hash) *dtypes.BlockStorageDiff {
	stateDiff := &dtypes.BlockStorageDiff{}
	for addrhash := range bs.DeletedAccounts {
		stateDiff.DeletedAccounts = append(stateDiff.DeletedAccounts, addrhash)
	}
	for _, v := range bs.NewAccounts {
		stateDiff.NewAccounts = append(stateDiff.NewAccounts, v)
	}
	for account, storage := range bs.StorageDiff {
		Values := make([]dtypes.IndexValuePair, 0)
		for index, v := range storage {
			Values = append(Values, dtypes.IndexValuePair{
				Index: index,
				Value: v,
			})
		}
		stateDiff.StorageDiff = append(stateDiff.StorageDiff, dtypes.AccountStorageDiff{
			Address: account,
			Values:  Values,
		})
	}
	for _, code := range bs.NewCodes {
		stateDiff.NewCodes = append(stateDiff.NewCodes, code)
	}

	stateDiff.Hash = root
	stateDiff.ParentHash = parrentRoot
	return stateDiff

}

func (bs *BlockStorageDiffMap) UpdateAccountData(address common.Address, original, account *accounts.Account) error {
	addrhash := crypto.Keccak256Hash(address.Bytes())
	delete(bs.DeletedAccounts, addrhash)
	bs.NewAccounts[addrhash] = dtypes.NewAccount{
		Address:  addrhash,
		Balance:  account.Balance.Clone(),
		Nonce:    account.Nonce,
		CodeHash: account.CodeHash,
	}
	return nil
}

func (bs *BlockStorageDiffMap) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	bs.NewCodes[codeHash] = dtypes.NewCode{
		CodeHash: codeHash,
		Code:     code,
	}
	return nil
}

func (bs *BlockStorageDiffMap) DeleteAccount(address common.Address, original *accounts.Account) error {
	addrhash := crypto.Keccak256Hash(address.Bytes())
	delete(bs.NewAccounts, addrhash)
	bs.DeletedAccounts[addrhash] = struct{}{}
	return nil
}

func (bs *BlockStorageDiffMap) WriteAccountStorage(address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	addrhash := crypto.Keccak256Hash(address.Bytes())
	if _, ok := bs.StorageDiff[addrhash]; !ok {
		bs.StorageDiff[addrhash] = make(map[common.Hash]*uint256.Int)
	}
	storageDiff := bs.StorageDiff[addrhash]
	storageDiff[crypto.Keccak256Hash(key.Bytes())] = value
	bs.StorageChanges[address] = struct{}{}
	return nil
}

func (bs *BlockStorageDiffMap) CreateContract(address common.Address) error {
	return nil
}

func (bs *BlockStorageDiffMap) WriteChangeSets() error {
	return nil
}

func (bs *BlockStorageDiffMap) WriteHistory() error {
	return nil
}

type callFrame struct {
	Type         vm.OpCode       `json:"-"`
	From         common.Address  `json:"from"`
	Gas          uint64          `json:"gas"`
	GasUsed      uint64          `json:"gasUsed"`
	To           *common.Address `json:"to,omitempty" rlp:"optional"`
	Input        []byte          `json:"input" rlp:"optional"`
	Output       []byte          `json:"output,omitempty" rlp:"optional"`
	Error        string          `json:"error,omitempty" rlp:"optional"`
	RevertReason string          `json:"revertReason,omitempty"`
	ParentFailed bool
	Calls        []callFrame    `json:"calls,omitempty" rlp:"optional"`
	Logs         []dtypes.Event `json:"logs,omitempty" rlp:"optional"`

	PosInParentTrace  int    `json:"pos_in_parent_trace"`
	ParentTraceID     string `json:"parent_trace_id"`
	TraceID           string `json:"trace_id"`
	StorageChange     bool   `json:"storageChange"`
	SelfStorageChange bool   `json:"self_storage_change"`

	// Placed at end on purpose. The RLP will be decoded to 0 instead of
	// nil if there are non-empty elements after in the struct.
	Value *big.Int `json:"value,omitempty" rlp:"optional"`
}

func (f callFrame) TypeString() string {
	return f.Type.String()
}

func (f callFrame) failed() bool {
	return len(f.Error) > 0
}

func (f *callFrame) processOutput(output []byte, err error, reverted bool) {
	output = common.CopyBytes(output)
	// Clear error if tx wasn't reverted. This happened
	// for pre-homestead contract storage OOG.
	if err != nil && !reverted {
		err = nil
	}
	if err == nil {
		f.Output = output
		return
	}
	f.Error = err.Error()
	if f.Type == vm.CREATE || f.Type == vm.CREATE2 {
		f.To = nil
	}
	if !errors.Is(err, vm.ErrExecutionReverted) || len(output) == 0 {
		return
	}
	f.Output = output
	if len(output) < 4 {
		return
	}
	if unpacked, err := abi.UnpackRevert(output); err == nil {
		f.RevertReason = unpacked
	}
}

var _ vm.EVMLogger = (*callTracer)(nil)

func (t *callTracer) ToTrace(f *callFrame, traceAddress []int64) dtypes.Trace {
	CallCreateType := ""
	CallType := ""
	switch f.Type {
	case vm.CREATE, vm.CREATE2:
		CallCreateType = strings.ToLower(vm.CREATE.String())
	case vm.SELFDESTRUCT:
		CallCreateType = "suicide"
	case vm.CALL, vm.STATICCALL, vm.CALLCODE, vm.DELEGATECALL:
		CallCreateType = strings.ToLower(vm.CALL.String())
		CallType = strings.ToLower(f.Type.String())
	default:
		CallCreateType = "empty"
	}
	to := common.Address{}
	if f.To != nil {
		to = *f.To
	}
	value := big.NewInt(0)
	if f.Value != nil {
		value = f.Value
	}
	err := ""
	if f.failed() {
		err = f.Error
		if f.RevertReason != "" {
			err = fmt.Sprintf("%s: %s", f.Error, f.RevertReason)
		}
	}
	return dtypes.Trace{
		ID:                f.TraceID,
		From:              strings.ToLower(f.From.Hex()),
		Gas:               big.NewInt(int64(f.Gas)),
		Input:             (hexutility.Bytes)(f.Input),
		To:                strings.ToLower(to.Hex()),
		Value:             (*hexutil.Big)(value),
		GasUsed:           big.NewInt(int64(f.GasUsed)),
		Output:            (hexutility.Bytes)(f.Output),
		CallCreateType:    CallCreateType,
		CallType:          CallType,
		TxID:              t.txID,
		ParentTraceID:     f.ParentTraceID,
		PosInParentTrace:  int64(f.PosInParentTrace),
		SelfStorageChange: f.SelfStorageChange,
		StorageChange:     f.StorageChange,
		Subtraces:         int64(len(f.Calls)),
		TraceAddress:      traceAddress,
		Error:             err,
	}
}

type callTracer struct {
	callstack   []callFrame
	gasLimit    uint64
	txID        string
	Evm         *vm.EVM
	BlockFile   *dtypes.BlockFile
	PendingLogs []*types.Log // only for polygon TransferLogs

	ChangeContracts map[common.Address]struct{}
}

func NewCallTracer(BlockFile *dtypes.BlockFile, txID string) *callTracer {
	return &callTracer{
		BlockFile: BlockFile,
		txID:      txID,

		ChangeContracts: make(map[common.Address]struct{}),
	}
}

func (t *callTracer) CaptureTxStart(gasLimit uint64) {
	t.gasLimit = gasLimit
}

func (t *callTracer) CaptureStart(env *vm.EVM, from common.Address, to common.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	toCopy := to
	tpy := vm.CALL
	if create {
		tpy = vm.CREATE
	}
	call := callFrame{
		Type:  tpy,
		From:  from,
		To:    &toCopy,
		Input: common.CopyBytes(input),
		Gas:   gas,
		Value: value.ToBig(),
	}
	t.Evm = env
	t.callstack = append(t.callstack, call)
	if len(t.PendingLogs) > 0 {
		for _, logg := range t.PendingLogs {
			t.OnLog(logg)
		}
		t.PendingLogs = nil
	}
}
func (t *callTracer) CaptureEnter(typ vm.OpCode, from common.Address, to common.Address, precompile bool, create bool, input []byte, gas uint64, value *uint256.Int, code []byte) {
	toCopy := to
	call := callFrame{
		Type:  vm.OpCode(typ),
		From:  from,
		To:    &toCopy,
		Input: common.CopyBytes(input),
		Gas:   gas,
		Value: value.ToBig(),
	}
	t.callstack = append(t.callstack, call)
}

func (t *callTracer) CaptureExit(output []byte, usedGas uint64, err error) {
	var reverted bool
	if err != nil {
		reverted = true
	}
	if !t.Evm.ChainRules().IsHomestead && errors.Is(err, vm.ErrCodeStoreOutOfGas) {
		reverted = false
	}
	size := len(t.callstack)
	if size <= 1 {
		return
	}
	// Pop call.
	call := t.callstack[size-1]
	t.callstack = t.callstack[:size-1]
	size -= 1

	call.GasUsed = usedGas
	call.processOutput(output, err, reverted)
	call.PosInParentTrace = len(t.callstack[size-1].Calls) + len(t.callstack[size-1].Logs)
	t.callstack[size-1].Calls = append(t.callstack[size-1].Calls, call)
}

func (t *callTracer) CaptureEnd(output []byte, usedGas uint64, err error) {
	var reverted bool
	if err != nil {
		reverted = true
	}
	if !t.Evm.ChainRules().IsHomestead && errors.Is(err, vm.ErrCodeStoreOutOfGas) {
		reverted = false
	}
	if len(t.callstack) != 1 {
		return
	}
	t.callstack[0].GasUsed = usedGas
	t.callstack[0].processOutput(output, err, reverted)
}

func (t *callTracer) CaptureState(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, rData []byte, opDepth int, err error) {
	if op == vm.SSTORE {
		t.callstack[len(t.callstack)-1].SelfStorageChange = true
		t.callstack[len(t.callstack)-1].StorageChange = true
	}
}

func (t *callTracer) CaptureFault(pc uint64, op vm.OpCode, gas, cost uint64, scope *vm.ScopeContext, depth int, err error) {

}

func setParentFailed(cf *callFrame, parentFailed bool) {
	failed := cf.failed() || parentFailed
	for i := range cf.Calls {
		cf.Calls[i].ParentFailed = failed
		setParentFailed(&cf.Calls[i], failed)
	}
}

func (t *callTracer) setStorageChange(cf *callFrame) {
	if cf.To != nil && cf.SelfStorageChange {
		t.ChangeContracts[*cf.To] = struct{}{}
	}
	subCallStorageChange := false
	for i := range cf.Calls {
		t.setStorageChange(&cf.Calls[i])
		if cf.Calls[i].StorageChange && !cf.Calls[i].failed() {
			subCallStorageChange = true
		}
	}
	if subCallStorageChange {
		cf.StorageChange = true
	}
}

func (t *callTracer) CaptureTxEnd(restGas uint64) {
	setParentFailed(&t.callstack[0], false)
	t.setStorageChange(&t.callstack[0])
	if len(t.callstack) == 1 {
		topCall := &t.callstack[0]
		topCall.TraceID = util.ToHash([]string{t.txID, "", "0"})
		if topCall.failed() {
			t.BlockFile.ErrorTraces = append(t.BlockFile.ErrorTraces, t.ToTrace(topCall, []int64{}))
		} else {
			t.BlockFile.Traces = append(t.BlockFile.Traces, t.ToTrace(topCall, []int64{}))
		}
		t.addTraceAndLog(topCall, []int64{})
	}
}

func (t *callTracer) addTraceAndLog(cf *callFrame, traceAddress []int64) {
	for i := range cf.Calls {
		cf.Calls[i].ParentTraceID = cf.TraceID
		cf.Calls[i].TraceID = util.ToHash([]string{t.txID, cf.TraceID, fmt.Sprintf("%d", cf.Calls[i].PosInParentTrace)})
		t.addTraceAndLog(&cf.Calls[i], childTraceAddress(traceAddress, int64(i)))
	}
	for i := range cf.Logs {
		cf.Logs[i].ParentTraceID = cf.TraceID
		cf.Logs[i].ID = util.ToHash([]string{cf.Logs[i].ParentTraceID, fmt.Sprintf("%d", cf.Logs[i].Position)})
		if cf.failed() || cf.ParentFailed {
			cf.Logs[i].LogIndex = 0
			t.BlockFile.ErrorEvents = append(t.BlockFile.ErrorEvents, cf.Logs[i])
		} else {
			t.BlockFile.Events = append(t.BlockFile.Events, cf.Logs[i])
		}
	}
	for i := range cf.Calls {
		if cf.Calls[i].failed() {
			t.BlockFile.ErrorTraces = append(t.BlockFile.ErrorTraces, t.ToTrace(&cf.Calls[i], childTraceAddress(traceAddress, int64(i))))
		} else {
			t.BlockFile.Traces = append(t.BlockFile.Traces, t.ToTrace(&cf.Calls[i], childTraceAddress(traceAddress, int64(i))))
		}
	}
}

func (t *callTracer) OnLog(logg *types.Log) {
	if len(t.callstack) == 0 {
		t.PendingLogs = append(t.PendingLogs, logg)
		return
	}
	topics := make([]string, len(logg.Topics))
	for i, topic := range logg.Topics {
		topics[i] = topic.Hex()
	}
	var selector string
	var remainingTopics []string

	if len(topics) > 0 {
		selector = topics[0]
		remainingTopics = topics[1:]
	}
	l := dtypes.Event{
		Address:  strings.ToLower(logg.Address.Hex()),
		Selector: selector,
		Topics:   remainingTopics,
		Data:     logg.Data,
		Position: int64(len(t.callstack[len(t.callstack)-1].Calls) + len(t.callstack[len(t.callstack)-1].Logs)),
		LogIndex: int64(logg.Index),
	}
	t.callstack[len(t.callstack)-1].Logs = append(t.callstack[len(t.callstack)-1].Logs, l)
}

func OnGenesisBlock(block *types.Block, alloc types.GenesisAlloc) (*dtypes.DebankOutPut, error) {
	// do something

	header := BuildPilelineBlockHeader(block)
	blockDiff := GenesisAllocToStateDiff(alloc)
	blockDiff.Hash = block.Root()
	blockDiff.ParentHash = types.EmptyRootHash

	blockFile := &dtypes.BlockFile{
		Block:            BuildPipelineBlock(block),
		Txs:              make([]dtypes.Transaction, 0),
		Events:           make([]dtypes.Event, 0),
		Traces:           make([]dtypes.Trace, 0),
		ErrorEvents:      make([]dtypes.Event, 0),
		ErrorTraces:      make([]dtypes.Trace, 0),
		StorageContracts: make([]string, 0),
	}

	for addr, acc := range alloc {
		if len(acc.Storage) > 0 {
			blockFile.StorageContracts = append(blockFile.StorageContracts, strings.ToLower(addr.Hex()))
		}
	}
	return &dtypes.DebankOutPut{
		BlockFile:      blockFile,
		Header:         header,
		StateDiff:      blockDiff,
		ValidationHash: blockFile.Validation().ValidationHash,
	}, nil
}

func GenesisAllocToStateDiff(genesisAlloc types.GenesisAlloc) *dtypes.BlockStorageDiff {
	diff := &dtypes.BlockStorageDiff{}
	diff.NewAccounts = make([]dtypes.NewAccount, 0)
	diff.NewCodes = make([]dtypes.NewCode, 0)
	diff.StorageDiff = make([]dtypes.AccountStorageDiff, 0)
	diff.DeletedAccounts = make([]common.Hash, 0)
	for addr, acc := range genesisAlloc {
		diff.NewAccounts = append(diff.NewAccounts, dtypes.NewAccount{
			Address:  crypto.Keccak256Hash(addr[:]),
			Balance:  uint256.MustFromBig(acc.Balance),
			Nonce:    acc.Nonce,
			CodeHash: common.BytesToHash(acc.Code),
		})
		if len(acc.Code) > 0 {
			diff.NewCodes = append(diff.NewCodes, dtypes.NewCode{
				CodeHash: common.BytesToHash(acc.Code),
				Code:     acc.Code,
			})
		}
		values := make([]dtypes.IndexValuePair, 0)
		for index, v := range acc.Storage {
			value := uint256.NewInt(0)
			if len(v) > 0 {
				value = uint256.NewInt(0).SetBytes(v.Bytes())
			}
			values = append(values, dtypes.IndexValuePair{
				Index: index,
				Value: value,
			})
		}
		diff.StorageDiff = append(diff.StorageDiff, dtypes.AccountStorageDiff{
			Address: crypto.Keccak256Hash(addr[:]),
			Values:  values,
		})
	}
	return diff
}

func childTraceAddress(a []int64, i int64) []int64 {
	child := make([]int64, 0, len(a)+1)
	child = append(child, a...)
	child = append(child, i)
	return child
}
