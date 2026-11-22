package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	maxBodyBytes  = 1 << 20
	maxCommentRunes = 300
)

type giteaUser struct {
	Login   string `json:"login"`
	HTMLURL string `json:"html_url"`
}

type giteaRepository struct {
	ID       int64     `json:"id"`
	Name     string    `json:"name"`
	FullName string    `json:"full_name"`
	HTMLURL  string    `json:"html_url"`
	Owner    giteaUser `json:"owner"`
}

type giteaIssue struct {
	Number      int        `json:"number"`
	Title       string     `json:"title"`
	Body        string     `json:"body"`
	HTMLURL     string     `json:"html_url"`
	User        giteaUser  `json:"user"`
	PullRequest *struct{}  `json:"pull_request"`
}

type giteaComment struct {
	Body    string    `json:"body"`
	HTMLURL string    `json:"html_url"`
	User    giteaUser `json:"user"`
}

// giteaPayload is the subset of the Gitea webhook body this bridge reads.
type giteaPayload struct {
	Action     string          `json:"action"`
	Issue      giteaIssue      `json:"issue"`
	Comment    giteaComment    `json:"comment"`
	Repository giteaRepository `json:"repository"`
	Sender     giteaUser       `json:"sender"`
}

// deliveryCache remembers recently accepted delivery IDs. Gitea retries a
// failed delivery; a retry must never publish twice.
type deliveryCache struct {
	mu      sync.Mutex
	entries map[string]time.Time
	order   []string
	ttl     time.Duration
	max     int
}

func newDeliveryCache(max int, ttl time.Duration) *deliveryCache {
	return &deliveryCache{entries: make(map[string]time.Time), ttl: ttl, max: max}
}

// add records id and reports whether it was new.
func (c *deliveryCache) add(id string, now time.Time) bool {
	if id == "" {
		return true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.evict(now)
	if _, ok := c.entries[id]; ok {
		return false
	}
	if len(c.order) >= c.max {
		oldest := c.order[0]
		c.order = c.order[1:]
		delete(c.entries, oldest)
	}
	c.entries[id] = now
	c.order = append(c.order, id)
	return true
}

// forget drops id again, so a delivery that could not be published at all can
// be retried by Gitea later.
func (c *deliveryCache) forget(id string) {
	if id == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.entries[id]; !ok {
		return
	}
	delete(c.entries, id)
	for i, v := range c.order {
		if v == id {
			c.order = append(c.order[:i], c.order[i+1:]...)
			break
		}
	}
}

func (c *deliveryCache) evict(now time.Time) {
	cutoff := now.Add(-c.ttl)
	kept := c.order[:0]
	for _, id := range c.order {
		if t, ok := c.entries[id]; ok && t.After(cutoff) {
			kept = append(kept, id)
			continue
		}
		delete(c.entries, id)
	}
	c.order = kept
}

type hookHandler struct {
	cfg    config
	pub    *publisher
	dedupe *deliveryCache
}

func newHookHandler(cfg config, pub *publisher, dedupe *deliveryCache) http.Handler {
	return &hookHandler{
		cfg:    cfg,
		pub:    pub,
		dedupe: dedupe,
	}
}

func (h *hookHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
		return
	}

	if h.cfg.webhookSecret != "" {
		sig := r.Header.Get("X-Gitea-Signature")
		if !validSignature(h.cfg.webhookSecret, body, sig) {
			slog.Warn("rejected delivery with bad signature", "remote", r.RemoteAddr)
			http.Error(w, "invalid signature", http.StatusUnauthorized)
			return
		}
	}

	var p giteaPayload
	if err := json.Unmarshal(body, &p); err != nil {
		slog.Warn("malformed payload", "error", err)
		http.Error(w, "malformed payload", http.StatusBadRequest)
		return
	}

	delivery := r.Header.Get("X-Gitea-Delivery")
	eventType := r.Header.Get("X-Gitea-Event")

	// Dedupe before anything else: a Gitea retry is cheap to acknowledge and
	// must not double-push.
	if !h.dedupe.add(delivery, time.Now()) {
		slog.Info("duplicate delivery ignored", "delivery", delivery)
		w.WriteHeader(http.StatusAccepted)
		return
	}

	if isIgnoredSender(p.Sender.Login, h.cfg.ignoreSenders) {
		slog.Info("own action ignored", "sender", p.Sender.Login, "event", eventType)
		w.WriteHeader(http.StatusAccepted)
		return
	}

	ev, reason := mapEvent(eventType, &p)
	if ev == nil {
		slog.Info("event has no notification", "reason", reason, "event", eventType, "action", p.Action)
		w.WriteHeader(http.StatusAccepted)
		return
	}
	ev.Delivery = delivery

	if !h.pub.enqueue(ev) {
		// Queue full: drop the dedupe entry so Gitea's retry can succeed
		// later, and tell Gitea to retry.
		h.dedupe.forget(delivery)
		slog.Warn("publish queue full; asking Gitea to retry", "repo", ev.Repo)
		http.Error(w, "queue full", http.StatusServiceUnavailable)
		return
	}

	slog.Info("accepted event", "event", eventType, "action", p.Action, "repo", ev.Repo, "delivery", delivery, "took", time.Since(start))
	w.WriteHeader(http.StatusAccepted)
}

// mapEvent turns a Gitea payload into a notification. A nil event means the
// event deliberately publishes nothing; reason explains why in the log.
func mapEvent(eventType string, p *giteaPayload) (*event, string) {
	if p.Repository.FullName == "" {
		return nil, "no repository"
	}
	switch eventType {
	case "issues":
		if p.Action != "opened" {
			return nil, "action " + p.Action
		}
		issue := p.Issue
		body := fmt.Sprintf("%s\n\nby @%s\n%s", strings.TrimSpace(issue.Title), p.Sender.Login, issue.HTMLURL)
		return &event{
			Kind:     "issue",
			Repo:     p.Repository.FullName,
			RepoURL:  p.Repository.HTMLURL,
			Title:    fmt.Sprintf("new issue · %s#%d", p.Repository.FullName, issue.Number),
			Body:     body,
			Tags:     []string{"ticket"},
			Priority: 4,
			Click:    issue.HTMLURL,
		}, ""

	case "issue_comment":
		if p.Action != "created" {
			return nil, "action " + p.Action
		}
		// Pull request chatter is deliberately out of scope: the ask was
		// issues, and review traffic would drown it.
		if p.Issue.PullRequest != nil {
			return nil, "pull request comment"
		}
		text := truncateRunes(stripMarkdown(p.Comment.Body), maxCommentRunes)
		if text == "" {
			text = "(no text)"
		}
		click := p.Comment.HTMLURL
		if click == "" {
			click = p.Issue.HTMLURL
		}
		return &event{
			Kind:     "comment",
			Repo:     p.Repository.FullName,
			RepoURL:  p.Repository.HTMLURL,
			Title:    fmt.Sprintf("comment · %s#%d", p.Repository.FullName, p.Issue.Number),
			Body:     fmt.Sprintf("@%s: %s", p.Sender.Login, text),
			Tags:     []string{"speech_balloon"},
			Priority: 3,
			Click:    click,
		}, ""
	}
	return nil, "event type " + eventType
}

func isIgnoredSender(login string, ignored []string) bool {
	for _, s := range ignored {
		if strings.EqualFold(strings.TrimSpace(s), login) {
			return true
		}
	}
	return false
}

func validSignature(secret string, body []byte, sig string) bool {
	sig = strings.TrimSpace(sig)
	if sig == "" {
		return false
	}
	want := hmac.New(sha256.New, []byte(secret))
	want.Write(body)
	expected := hex.EncodeToString(want.Sum(nil))
	return subtle.ConstantTimeCompare([]byte(strings.ToLower(expected)), []byte(strings.ToLower(sig))) == 1
}

// stripMarkdown removes the most common markdown syntax so a comment reads as
// one plain line in a phone notification. It is intentionally best-effort: a
// stray character costs nothing, an unreadable template does.
func stripMarkdown(s string) string {
	var b strings.Builder
	b.Grow(len(s))

	// Code fences vanish; their content stays as text.
	lines := strings.Split(s, "\n")
	var inFence bool
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "```") || strings.HasPrefix(trimmed, "~~~") {
			inFence = !inFence
			continue
		}
		if !inFence {
			line = stripLineMarkdown(line)
		}
		b.WriteString(line)
		b.WriteString(" ")
	}
	return collapseSpace(b.String())
}

func stripLineMarkdown(line string) string {
	t := strings.TrimSpace(line)
	// Block quote markers and list bullets.
	for strings.HasPrefix(t, ">") {
		t = strings.TrimSpace(strings.TrimPrefix(t, ">"))
	}
	for strings.HasPrefix(t, "- ") || strings.HasPrefix(t, "* ") || strings.HasPrefix(t, "+ ") {
		t = strings.TrimSpace(t[2:])
	}
	// Headings.
	t = strings.TrimLeft(t, "#")
	t = strings.TrimSpace(t)

	var b strings.Builder
	b.Grow(len(t))
	inCode := false
	for i := 0; i < len(t); {
		c := t[i]
		if c == '`' {
			inCode = !inCode
			i++
			continue
		}
		if inCode {
			b.WriteByte(c)
			i++
			continue
		}
		switch {
		case c == '!' && i+1 < len(t) && t[i+1] == '[':
			// Images carry no useful text in a notification: drop alt text
			// and target together.
			i = skipLink(t, i+1)
		case c == '[':
			// Markdown link: keep the label, drop brackets and target.
			end := linkLabelEnd(t, i)
			if end < 0 {
				i++
				continue
			}
			b.WriteString(stripEmphasis(t[i+1 : end]))
			after := end + 1
			if after < len(t) && t[after] == '(' {
				after = skipParens(t, after)
			}
			i = after
		case c == '<':
			// HTML tag: skip it entirely.
			if end := strings.IndexByte(t[i:], '>'); end >= 0 {
				i += end + 1
			} else {
				i++
			}
		case c == '*' || c == '_' || c == '#' || c == '~':
			// Emphasis and strikethrough markers.
			i++
		default:
			b.WriteByte(c)
			i++
		}
	}
	return b.String()
}

// linkLabelEnd returns the index of the ']' that closes the label opened at
// start ('['), or -1.
func linkLabelEnd(t string, start int) int {
	depth := 0
	for i := start; i < len(t); i++ {
		switch t[i] {
		case '[':
			depth++
		case ']':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// skipParens returns the index just past the ')' matching the '(' at start.
func skipParens(t string, start int) int {
	depth := 0
	for i := start; i < len(t); i++ {
		switch t[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i + 1
			}
		}
	}
	return len(t)
}

// skipLink drops a link or image starting at the '[' at start.
func skipLink(t string, start int) int {
	end := linkLabelEnd(t, start)
	if end < 0 {
		return start + 1
	}
	after := end + 1
	if after < len(t) && t[after] == '(' {
		after = skipParens(t, after)
	}
	return after
}

// stripEmphasis removes emphasis markers from a link label.
func stripEmphasis(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '*', '_', '~':
			continue
		default:
			b.WriteByte(s[i])
		}
	}
	return b.String()
}

func collapseSpace(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	space := false
	for _, r := range s {
		if unicode.IsSpace(r) {
			if !space {
				b.WriteRune(' ')
			}
			space = true
			continue
		}
		space = false
		b.WriteRune(r)
	}
	return strings.TrimSpace(b.String())
}

func truncateRunes(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	r := []rune(s)
	return strings.TrimSpace(string(r[:n]))
}
