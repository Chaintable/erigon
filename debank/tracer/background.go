package tracer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/erigontech/erigon-lib/log/v3"
	dtypes "github.com/erigontech/erigon/debank/types"
	"github.com/erigontech/erigon/debank/util"
	"github.com/erigontech/erigon/rpc"
)

var DebankTraceBackGroundMangeInstance *DebankTraceManger

type DebankTrace interface {
	DebankBlockRaw(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (*dtypes.DebankOutPut, error)
}

type DebankOutUploader interface {
	UploadDebankOutPut(ctx context.Context, out *dtypes.DebankOutPut) error

	PushDebankOutPut(ctx context.Context, out *dtypes.DebankOutPut) error
}

func init() {
	DebankTraceBackGroundMangeInstance = &DebankTraceManger{}
}

type result struct {
	output *dtypes.DebankOutPut
	err    error
}

type TraceTaskHandle struct {
	blockNumber uint64
	done        chan *result
}

func (t *TraceTaskHandle) run(ctx context.Context, api DebankTrace, uploader DebankOutUploader) {
	blockNumber := rpc.BlockNumber(t.blockNumber)
	defer close(t.done)
	bno := rpc.BlockNumberOrHash{
		BlockNumber:      &blockNumber,
		RequireCanonical: true,
	}
	output, err := api.DebankBlockRaw(ctx, bno)
	if err != nil {
		res := &result{
			output: nil,
			err:    err,
		}
		t.done <- res
		return
	}
	err = uploader.UploadDebankOutPut(ctx, output)
	if err != nil {
		res := &result{
			output: nil,
			err:    err,
		}
		t.done <- res
		return
	}
	res := &result{
		output: output,
		err:    nil,
	}
	t.done <- res
}

type DebankTraceTaskGroup struct {
	startBlock  uint64
	endBlock    uint64
	latestBlock uint64
	maxTask     uint64
	startTime   time.Time
	channels    []TraceTaskHandle
	sync.Mutex
	quit chan struct{}
}

func NewDebankTraceTaskGroup(startBlock, endBlock, maxTask uint64) *DebankTraceTaskGroup {
	return &DebankTraceTaskGroup{
		startBlock: startBlock,
		endBlock:   endBlock,
		maxTask:    maxTask,
		quit:       make(chan struct{}),
	}
}

func (t *DebankTraceTaskGroup) Stop() {
	close(t.quit)
}

func (t *DebankTraceTaskGroup) clear() {
	DebankTraceBackGroundMangeInstance.Lock()
	defer DebankTraceBackGroundMangeInstance.Unlock()
	DebankTraceBackGroundMangeInstance.currentTaskGroup = nil
}

func (t *DebankTraceTaskGroup) start(api DebankTrace, uploader DebankOutUploader) {
	t.startTime = time.Now()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer t.clear()
	for i := t.startBlock; i < t.startBlock+t.maxTask; i++ {
		t.channels = append(t.channels, TraceTaskHandle{
			blockNumber: i,
			done:        make(chan *result),
		})
		go t.channels[len(t.channels)-1].run(ctx, api, uploader)
	}
	for {
		select {
		case <-t.quit:
			return
		case res := <-t.channels[0].done:
			if res.err != nil {
				log.Error("DebankBlock error", "err", res.err, "blockNumber", t.channels[0].blockNumber)
				return
			}
			t.Lock()
			t.latestBlock = t.channels[0].blockNumber
			t.Unlock()
			err := uploader.PushDebankOutPut(ctx, res.output)
			if err != nil {
				log.Error("PushDebankOutPut error", "err", err)
				return
			}
			if t.latestBlock == t.endBlock {
				return
			}
			t.channels = t.channels[1:]
			if t.latestBlock+t.maxTask <= t.endBlock {
				t.channels = append(t.channels, TraceTaskHandle{
					blockNumber: t.latestBlock + t.maxTask,
					done:        make(chan *result),
				})
				go t.channels[len(t.channels)-1].run(ctx, api, uploader)
			}
		}
	}
}

type DebankTraceManger struct {
	currentTaskGroup *DebankTraceTaskGroup
	sync.Mutex
}

func (m *DebankTraceManger) Start(api DebankTrace, region string, nodeXBucket string, chainTableBucket string, broker string, topic string, chainID string, startBlock, endBlock, maxTask uint64) error {
	m.Lock()
	defer m.Unlock()
	if m.currentTaskGroup != nil {
		log.Error("DebankTraceManger is already running")
		return fmt.Errorf("DebankTraceManger is already running")
	}
	m.currentTaskGroup = NewDebankTraceTaskGroup(startBlock, endBlock, maxTask)
	uploader, err := util.NewUploader(region, nodeXBucket, chainTableBucket, broker, topic, chainID)
	if err != nil {
		log.Error("NewUploader error", "err", err)
		return err
	}
	go m.currentTaskGroup.start(api, uploader)
	return nil
}

func (m *DebankTraceManger) Stop() {
	m.Lock()
	defer m.Unlock()
	if m.currentTaskGroup != nil {
		m.currentTaskGroup.Stop()
		m.currentTaskGroup = nil
	}
}

func (m *DebankTraceManger) Status() (uint64, uint64, uint64, time.Time) {
	m.Lock()
	defer m.Unlock()
	if m.currentTaskGroup == nil {
		return 0, 0, 0, time.Time{}
	}
	m.currentTaskGroup.Lock()
	defer m.currentTaskGroup.Unlock()
	return m.currentTaskGroup.startBlock, m.currentTaskGroup.endBlock, m.currentTaskGroup.latestBlock, m.currentTaskGroup.startTime
}
