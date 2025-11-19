package native

import (
	"encoding/json"
	"fmt"

	"github.com/erigontech/erigon/eth/tracers"
)

func init() {
	tracers.RegisterLookup(true, flatAliasLookup)
}

// flatAliasLookup maps "flatCallTracer" ("flat_call_tracer") to the native call tracer
// and forces a flat output via config, while preserving any user-supplied tracerConfig.
func flatAliasLookup(name string, ctx *tracers.Context, userCfg json.RawMessage) (*tracers.Tracer, error) {
	if name != "flatCallTracer" && name != "flat_call_tracer" {
		// not handled by this lookup, hence we let others try
		return nil, nil
	}

	// Defaults to coerce the call tracer into flat mode
	defaults := json.RawMessage(`{"flat":true,"onlyTopCall":true}`)

	merged, err := mergeJSON(defaults, userCfg)
	if err != nil {
		return nil, fmt.Errorf("flatCallTracer: config merge failed: %w", err)
	}

	// Delegate to the native call tracer with the new merged config.
	return newCallTracer(ctx, merged)
}

// mergeJSON does a shallow object merge of b over a.
// If either is empty, returns the other.
func mergeJSON(a, b json.RawMessage) (json.RawMessage, error) {
	if len(a) == 0 {
		return b, nil
	}
	if len(b) == 0 {
		return a, nil
	}
	var ma, mb map[string]any
	if err := json.Unmarshal(a, &ma); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(b, &mb); err != nil {
		return nil, err
	}
	for k, v := range mb {
		ma[k] = v
	}
	out, err := json.Marshal(ma)
	return out, err
}
