package metrics

import (
	"fmt"
	"net/http"

	"github.com/thorOdinson16/multiplayer-infra/services/analytics-service/internal/consumer"
)

func Handler(m *consumer.Metrics) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		m.Lock()
		defer m.Unlock()
		fmt.Fprintf(w, "# HELP telemetry_events_total Total telemetry events\n")
		fmt.Fprintf(w, "# TYPE telemetry_events_total counter\n")
		fmt.Fprintf(w, "telemetry_events_total %d\n", m.TotalEvents)
		fmt.Fprintf(w, "# HELP analytics_matches_total Total matches\n")
		fmt.Fprintf(w, "# TYPE analytics_matches_total counter\n")
		fmt.Fprintf(w, "analytics_matches_total %d\n", m.TotalMatches)
		fmt.Fprintf(w, "# HELP analytics_active_matches Currently active matches\n")
		fmt.Fprintf(w, "# TYPE analytics_active_matches gauge\n")
		fmt.Fprintf(w, "analytics_active_matches %d\n", len(m.ActiveMatches))
	}
}
