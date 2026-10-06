package agent

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// Alerter logs events that need a human at ERROR and, if a URL is configured,
// POSTs them (ntfy-compatible: plain-text body, Title header). Repeats of the
// same event key are suppressed for an hour so a stuck condition pages once.
type Alerter struct {
	url     string
	service string
	log     *slog.Logger
	client  *http.Client

	mu   sync.Mutex
	last map[string]time.Time
}

func NewAlerter(url, service string, log *slog.Logger) *Alerter {
	return &Alerter{url: url, service: service, log: log, client: &http.Client{Timeout: 5 * time.Second}, last: map[string]time.Time{}}
}

func (al *Alerter) Alert(event, msg string, kv ...any) {
	al.log.Error("ALERT "+event+": "+msg, kv...)
	if al.url == "" {
		return
	}
	key := fmt.Sprint(event, kv)
	al.mu.Lock()
	if t, ok := al.last[key]; ok && time.Since(t) < time.Hour {
		al.mu.Unlock()
		return
	}
	al.last[key] = time.Now()
	al.mu.Unlock()
	body := msg + "\n" + fmt.Sprint(kv...)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, al.url, bytes.NewBufferString(body))
		if err != nil {
			return
		}
		req.Header.Set("Title", fmt.Sprintf("singleton/%s: %s", al.service, event))
		if resp, err := al.client.Do(req); err == nil {
			resp.Body.Close()
		} else {
			al.log.Warn("alert delivery failed", "err", err)
		}
	}()
}
