package local

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/sol-strategies/solana-validator-ha/internal/config"
	"github.com/sol-strategies/solana-validator-ha/internal/rpc"
	"github.com/stretchr/testify/require"
)

// Use actual JSON-RPC requests so commitment handling and RPC failures are
// exercised alongside the readiness policy.
func slotRPC(t *testing.T, slots map[string]uint64) *rpc.Client {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     int    `json:"id"`
			Method string `json:"method"`
			Params []struct {
				Commitment string `json:"commitment"`
			} `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		reply := map[string]interface{}{"jsonrpc": "2.0", "id": req.ID}
		switch req.Method {
		case "getHealth":
			reply["result"] = "ok"
		case "getSlot":
			if len(req.Params) != 1 {
				t.Error("getSlot must specify a commitment")
				http.Error(w, "missing commitment", http.StatusBadRequest)
				return
			}
			if slot, ok := slots[req.Params[0].Commitment]; ok {
				reply["result"] = slot
			} else {
				reply["error"] = map[string]interface{}{"code": -32005, "message": "slot unavailable"}
			}
		default:
			reply["error"] = map[string]interface{}{"code": -32601, "message": "unexpected method"}
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(reply); err != nil {
			t.Error(err)
		}
	}))
	t.Cleanup(server.Close)
	return rpc.NewClient("slot-health-test", server.URL)
}

func TestSlotHealthRejectsHealthyButLaggingOrUnverifiableNode(t *testing.T) {
	tests := []struct {
		name           string
		local, cluster map[string]uint64
		healthy        bool
	}{
		{"caught up", map[string]uint64{"processed": 10000, "finalized": 9968}, map[string]uint64{"processed": 10001, "finalized": 9969}, true},
		{"healthy during restore backlog", map[string]uint64{"processed": 1748, "finalized": 1716}, map[string]uint64{"processed": 10000, "finalized": 9968}, false},
		{"processed caught up but finalized stalled", map[string]uint64{"processed": 10000, "finalized": 9000}, map[string]uint64{"processed": 10000, "finalized": 9968}, false},
		{"local slot unavailable", map[string]uint64{}, map[string]uint64{"processed": 10000, "finalized": 9968}, false},
		{"cluster slot unavailable", map[string]uint64{"processed": 10000, "finalized": 9968}, map[string]uint64{}, false},
		{"cluster reference too far behind", map[string]uint64{"processed": 10000, "finalized": 9968}, map[string]uint64{"processed": 9000, "finalized": 8968}, false},
		{"at configured boundary", map[string]uint64{"processed": 9968, "finalized": 9936}, map[string]uint64{"processed": 10000, "finalized": 9968}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewState(Options{RPC: slotRPC(t, tt.local), ClusterRPC: slotRPC(t, tt.cluster),
				Cfg: config.SelfHealthy{MaxSlotDistance: 32}, Ctx: context.Background()})
			require.Equal(t, tt.healthy, s.IsSelfHealthy())
		})
	}
}

func TestPassiveIdentityMustMatchConfiguredKey(t *testing.T) {
	server := newMockRPCServer(t, map[string]interface{}{
		"getIdentity": identityResult("11111111111111111111111111111111"),
	})
	s := newTestState(rpc.NewClient("test", server.URL), 0)
	require.False(t, s.IsSelfPassive(), "an unrelated identity is not a passive member of this pair")
}

func TestHealthRequiresClusterReference(t *testing.T) {
	s := NewState(Options{RPC: slotRPC(t, map[string]uint64{"processed": 10000, "finalized": 9968}), Ctx: context.Background()})
	require.False(t, s.IsSelfHealthy())
}

func TestHealthConcurrentSampling(t *testing.T) {
	s := NewState(Options{
		RPC:        slotRPC(t, map[string]uint64{"processed": 10000, "finalized": 9968}),
		ClusterRPC: slotRPC(t, map[string]uint64{"processed": 10000, "finalized": 9968}),
		Cfg:        config.SelfHealthy{MaxSlotDistance: 32}, Ctx: context.Background(),
	})
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 3 {
				if !s.IsSelfHealthy() {
					t.Error("concurrent head check failed")
				}
			}
		})
	}
	wg.Wait()
}
