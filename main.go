// Command git-issue-notify bridges Gitea issue events to ntfy.
//
// It receives Gitea webhooks on POST /hook and publishes a push notification
// to an ntfy topic. It also reconciles the webhook configuration of every
// repository on the forge, so a repository created tomorrow is covered without
// anyone touching a settings page.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

// version is stamped by the release build.
var version = "dev"

type config struct {
	ntfyURL       string
	ntfyTopic     string
	webhookSecret string
	giteaURL      string
	giteaToken    string
	ignoreSenders []string
	repoExclude   []string
	reconcileFreq time.Duration
}

func env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func splitList(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func loadConfig() config {
	freq := 10 * time.Minute
	if v := env("RECONCILE_INTERVAL", ""); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			freq = d
		} else if v != "" {
			slog.Warn("ignoring invalid RECONCILE_INTERVAL", "value", v)
		}
	}
	return config{
		ntfyURL:       strings.TrimRight(env("NTFY_URL", "http://ntfy.default.svc.cluster.local"), "/"),
		ntfyTopic:     env("NTFY_TOPIC", "forge"),
		webhookSecret: os.Getenv("WEBHOOK_SECRET"),
		giteaURL:      strings.TrimRight(env("GITEA_URL", "http://gitea.default.svc.cluster.local:3000"), "/"),
		giteaToken:    os.Getenv("GITEA_TOKEN"),
		ignoreSenders: splitList(env("IGNORE_SENDERS", "naqerl")),
		repoExclude:   splitList(env("REPO_EXCLUDE", "grok/*")),
		reconcileFreq: freq,
	}
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))
	cfg := loadConfig()

	dedupe := newDeliveryCache(1024, 24*time.Hour)
	pub := newPublisher(cfg.ntfyURL, cfg.ntfyTopic, dedupe)
	hookHandler := newHookHandler(cfg, pub, dedupe)

	mux := http.NewServeMux()
	mux.Handle("POST /hook", hookHandler)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	srv := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go pub.run(ctx)

	if cfg.giteaToken != "" {
		rec := newReconciler(cfg)
		go rec.run(ctx, cfg.reconcileFreq)
	} else {
		slog.Warn("GITEA_TOKEN is empty; webhook reconciliation is disabled")
	}

	go func() {
		slog.Info("listening", "addr", srv.Addr, "version", version, "topic", cfg.ntfyTopic)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("http server failed", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Warn("graceful shutdown failed", "error", err)
	}
}
