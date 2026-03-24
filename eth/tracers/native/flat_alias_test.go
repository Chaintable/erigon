package native

import (
	"encoding/json"
	"testing"

	"github.com/erigontech/erigon/eth/tracers"
)

func TestMergeJSON_BothNonEmpty_ShallowOverlay(t *testing.T) {
	a := json.RawMessage(`{"flat":true,"onlyTopCall":true,"keepMe":1,"overrideMe":1}`)
	b := json.RawMessage(`{"convertParityErrors":true,"overrideMe":2}`)

	out, err := mergeJSON(a, b)
	if err != nil {
		t.Fatalf("mergeJSON error: %v", err)
	}

	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatalf("unmarshal merged json: %v", err)
	}

	// Defaults preserved
	if v, ok := m["flat"].(bool); !ok || !v {
		t.Fatalf("expected flat=true, got: %#v", m["flat"])
	}
	if v, ok := m["onlyTopCall"].(bool); !ok || !v {
		t.Fatalf("expected onlyTopCall=true, got: %#v", m["onlyTopCall"])
	}
	if v, ok := m["keepMe"].(float64); !ok || v != 1 {
		t.Fatalf("expected keepMe=1, got: %#v", m["keepMe"])
	}

	// New field merged
	if v, ok := m["convertParityErrors"].(bool); !ok || !v {
		t.Fatalf("expected convertParityErrors=true, got: %#v", m["convertParityErrors"])
	}

	// Overlay takes precedence
	if v, ok := m["overrideMe"].(float64); !ok || v != 2 {
		t.Fatalf("expected overrideMe=2, got: %#v", m["overrideMe"])
	}
}

func TestMergeJSON_HandlesEmptySides(t *testing.T) {
	aOnly, err := mergeJSON(json.RawMessage(`{"a":1}`), nil)
	if err != nil {
		t.Fatalf("mergeJSON aOnly err: %v", err)
	}
	if string(aOnly) != `{"a":1}` {
		t.Fatalf("unexpected aOnly: %s", string(aOnly))
	}

	bOnly, err := mergeJSON(nil, json.RawMessage(`{"b":2}`))
	if err != nil {
		t.Fatalf("mergeJSON bOnly err: %v", err)
	}
	if string(bOnly) != `{"b":2}` {
		t.Fatalf("unexpected bOnly: %s", string(bOnly))
	}
}

func TestFlatCallTracer_LookupResolves(t *testing.T) {
	ctx := &tracers.Context{}

	for _, name := range []string{"flatCallTracer", "flat_call_tracer"} {
		t.Run(name, func(t *testing.T) {
			cfg := json.RawMessage(`{"convertParityErrors":true}`)
			tr, err := tracers.New(name, ctx, cfg)
			if err != nil {
				t.Fatalf("tracers.New(%s) returned error: %v", name, err)
			}
			if tr == nil {
				t.Fatalf("tracers.New(%s) returned nil tracer", name)
			}
			if tr.Stop != nil {
				tr.Stop(nil)
			}
		})
	}
}

func TestFlatCallTracer_Smoke_NoPanicOnStopAndGetResult(t *testing.T) {
	ctx := &tracers.Context{}
	tr, err := tracers.New("flatCallTracer", ctx, json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("construct flatCallTracer: %v", err)
	}
	if tr.GetResult != nil {
		_, _ = tr.GetResult()
	}
	if tr.Stop != nil {
		tr.Stop(nil)
	}
}
