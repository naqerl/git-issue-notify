package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	queueSize       = 100
	publishInterval = 2 * time.Second
	retryWindow     = 5 * time.Minute
)

// event is one notification to publish.
type event struct {
	Kind     string // "issue" or "comment"
	Repo     string
	RepoURL  string
	Title    string
	Body     string
	Tags     []string
	Priority int
	Click    string
	Delivery string
}

type publisher struct {
	url    string
	topic  string
	queue  chan *event
	tokens chan struct{}
	client *http.Client
	dedupe *deliveryCache
}

func newPublisher(ntfyURL, topic string, dedupe *deliveryCache) *publisher {
	p := &publisher{
		url:   ntfyURL,
		topic: topic,
		queue: make(chan *event, queueSize),
		// One token is available at start so the first event is not delayed;
		// thereafter the refill is one token per publishInterval. ntfy limits
		// a source IP to a burst of 60 and one refill per 5 s; this bridge
		// publishes from its own pod IP and stays well below that.
		tokens: make(chan struct{}, 1),
		client: &http.Client{Timeout: 10 * time.Second},
		dedupe: dedupe,
	}
	p.tokens <- struct{}{}
	return p
}

// enqueue accepts an event, or reports false when the bounded queue is full.
func (p *publisher) enqueue(ev *event) bool {
	select {
	case p.queue <- ev:
		return true
	default:
		return false
	}
}

func (p *publisher) run(ctx context.Context) {
	refill := time.NewTicker(publishInterval)
	defer refill.Stop()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-refill.C:
				select {
				case p.tokens <- struct{}{}:
				default:
				}
			}
		}
	}()

	for {
		var first *event
		select {
		case <-ctx.Done():
			return
		case first = <-p.queue:
		}

		// Drain whatever else is already waiting. A burst becomes one
		// coalesced message per repository instead of one message per event,
		// which keeps both the queue and ntfy's rate limiter calm.
		batch := []*event{first}
		for len(batch) < queueSize {
			select {
			case ev := <-p.queue:
				batch = append(batch, ev)
			default:
				goto drained
			}
		}
	drained:

		if len(batch) > 1 {
			for repo, group := range groupByRepo(batch) {
				if err := p.publishWithRetry(ctx, coalesce(repo, group)); err != nil {
					p.giveUp(group, err)
				}
			}
			continue
		}
		for _, ev := range batch {
			if err := p.publishWithRetry(ctx, ev); err != nil {
				p.giveUp([]*event{ev}, err)
			}
		}
	}
}

func groupByRepo(batch []*event) map[string][]*event {
	groups := make(map[string][]*event, 4)
	for _, ev := range batch {
		groups[ev.Repo] = append(groups[ev.Repo], ev)
	}
	return groups
}

// coalesce summarises a burst for one repository in a single message. The
// count is part of the title so a glance at the phone already says how much
// was folded together.
func coalesce(repo string, group []*event) *event {
	var issues, comments int
	var click, repoURL string
	tags := make([]string, 0, 2)
	seenTag := make(map[string]bool, 2)
	priority := 0
	for _, ev := range group {
		switch ev.Kind {
		case "issue":
			issues++
		case "comment":
			comments++
		}
		if ev.Click != "" && click == "" {
			click = ev.Click
		}
		if ev.RepoURL != "" {
			repoURL = ev.RepoURL
		}
		if ev.Priority > priority {
			priority = ev.Priority
		}
		for _, t := range ev.Tags {
			if !seenTag[t] {
				seenTag[t] = true
				tags = append(tags, t)
			}
		}
	}
	var parts []string
	if issues > 0 {
		parts = append(parts, fmt.Sprintf("%d new issue(s)", issues))
	}
	if comments > 0 {
		parts = append(parts, fmt.Sprintf("%d comment(s)", comments))
	}
	body := fmt.Sprintf("%d events: %s", len(group), strings.Join(parts, ", "))
	return &event{
		Kind:     "coalesced",
		Repo:     repo,
		RepoURL:  repoURL,
		Title:    fmt.Sprintf("%d new events on %s", len(group), repo),
		Body:     body,
		Tags:     tags,
		Priority: priority,
		Click:    click,
	}
}

func (p *publisher) publishWithRetry(ctx context.Context, ev *event) error {
	deadline := time.Now().Add(retryWindow)
	backoff := time.Second
	var lastErr error
	for attempt := 1; ; attempt++ {
		if err := p.waitToken(ctx); err != nil {
			return err
		}
		err := p.publish(ctx, ev)
		if err == nil {
			return nil
		}
		lastErr = err
		// ctx cancellation is a shutdown, not a delivery failure.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) && ctx.Err() != nil {
			return err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("gave up after %d attempts: %w", attempt, lastErr)
		}
		slog.Warn("publish failed; retrying", "repo", ev.Repo, "attempt", attempt, "error", err, "backoff", backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
	}
}

func (p *publisher) waitToken(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.tokens:
		return nil
	}
}

func (p *publisher) publish(ctx context.Context, ev *event) error {
	endpoint := p.url + "/" + p.topic
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewBufferString(ev.Body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	req.Header.Set("Title", ev.Title)
	if len(ev.Tags) > 0 {
		req.Header.Set("Tags", strings.Join(ev.Tags, ","))
	}
	if ev.Priority > 0 {
		req.Header.Set("Priority", strconv.Itoa(ev.Priority))
	}
	if ev.Click != "" {
		req.Header.Set("Click", ev.Click)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		if resp.StatusCode == http.StatusTooManyRequests {
			slog.Warn("ntfy rate limited the bridge", "repo", ev.Repo)
		}
		return fmt.Errorf("ntfy answered %d", resp.StatusCode)
	}
	slog.Info("published", "repo", ev.Repo, "kind", ev.Kind, "title", ev.Title)
	return nil
}

// giveUp records why a batch could never be published and releases its
// delivery IDs so a later Gitea retry is not silently swallowed by dedupe.
func (p *publisher) giveUp(group []*event, err error) {
	slog.Error("dropping event batch after retry window", "count", len(group), "error", err)
	if p.dedupe == nil {
		return
	}
	for _, ev := range group {
		p.dedupe.forget(ev.Delivery)
	}
}
