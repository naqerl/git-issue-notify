package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"
)

const (
	hookEvents = "issues,issue_comment"
	hookURL    = "http://git-issue-notify.default.svc.cluster.local/hook"
	hookName   = "ntfy-issues"
)

type giteaRepo struct {
	ID        int64     `json:"id"`
	Name      string    `json:"name"`
	FullName  string    `json:"full_name"`
	HTMLURL   string    `json:"html_url"`
	Owner     giteaUser `json:"owner"`
	HasIssues bool      `json:"has_issues"`
	Mirror    bool      `json:"mirror"`
	Archived  bool      `json:"archived"`
}

type giteaHook struct {
	ID     int64             `json:"id"`
	Type   string            `json:"type"`
	Active bool              `json:"active"`
	Events []string          `json:"events"`
	Config map[string]string `json:"config"`
}

func (h *giteaHook) url() string {
	if h.Config == nil {
		return ""
	}
	return h.Config["url"]
}

type giteaClient struct {
	base  string
	token string
	http  *http.Client
}

func newGiteaClient(base, token string) *giteaClient {
	return &giteaClient{base: strings.TrimRight(base, "/"), token: token, http: &http.Client{Timeout: 30 * time.Second}}
}

func (c *giteaClient) do(ctx context.Context, method, p string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+p, body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "token "+c.token)
	req.Header.Set("Accept", "application/json")
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("%s %s: %d %s", method, p, resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	if out != nil {
		return json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(out)
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	return nil
}

func (c *giteaClient) listRepos(ctx context.Context) ([]giteaRepo, error) {
	const limit = 50
	var all []giteaRepo
	for page := 1; page <= 200; page++ {
		var resp struct {
			Data []giteaRepo `json:"data"`
		}
		p := fmt.Sprintf("/api/v1/repos/search?limit=%d&page=%d", limit, page)
		if err := c.do(ctx, http.MethodGet, p, nil, &resp); err != nil {
			return nil, err
		}
		all = append(all, resp.Data...)
		if len(resp.Data) < limit {
			break
		}
	}
	return all, nil
}

func (c *giteaClient) listHooks(ctx context.Context, owner, repo string) ([]giteaHook, error) {
	var hooks []giteaHook
	p := fmt.Sprintf("/api/v1/repos/%s/%s/hooks?limit=50", url.PathEscape(owner), url.PathEscape(repo))
	if err := c.do(ctx, http.MethodGet, p, nil, &hooks); err != nil {
		return nil, err
	}
	return hooks, nil
}

func hookConfig(secret string) map[string]string {
	cfg := map[string]string{
		"url":          hookURL,
		"content_type": "json",
		"http_method":  "post",
	}
	if secret != "" {
		cfg["secret"] = secret
	}
	return cfg
}

func (c *giteaClient) createHook(ctx context.Context, owner, repo, secret string) error {
	body := map[string]any{
		"type":   "gitea",
		"active": true,
		"events": []string{"issues", "issue_comment"},
		"config": hookConfig(secret),
	}
	p := fmt.Sprintf("/api/v1/repos/%s/%s/hooks", url.PathEscape(owner), url.PathEscape(repo))
	return c.do(ctx, http.MethodPost, p, body, nil)
}

func (c *giteaClient) editHook(ctx context.Context, owner, repo string, id int64, secret string) error {
	body := map[string]any{
		"active": true,
		"events": []string{"issues", "issue_comment"},
		"config": hookConfig(secret),
	}
	p := fmt.Sprintf("/api/v1/repos/%s/%s/hooks/%d", url.PathEscape(owner), url.PathEscape(repo), id)
	return c.do(ctx, http.MethodPatch, p, body, nil)
}

func (c *giteaClient) deleteHook(ctx context.Context, owner, repo string, id int64) error {
	p := fmt.Sprintf("/api/v1/repos/%s/%s/hooks/%d", url.PathEscape(owner), url.PathEscape(repo), id)
	return c.do(ctx, http.MethodDelete, p, nil, nil)
}

type reconciler struct {
	cfg config
	api *giteaClient
}

func newReconciler(cfg config) *reconciler {
	return &reconciler{cfg: cfg, api: newGiteaClient(cfg.giteaURL, cfg.giteaToken)}
}

func (r *reconciler) run(ctx context.Context, every time.Duration) {
	// First pass immediately, so a restart repairs a deleted hook without
	// waiting for the tick.
	r.once(ctx)
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.once(ctx)
		}
	}
}

func (r *reconciler) once(ctx context.Context) {
	started := time.Now()
	repos, err := r.api.listRepos(ctx)
	if err != nil {
		slog.Error("reconcile: list repositories failed", "error", err)
		return
	}
	var changed, skipped int
	for _, repo := range repos {
		if ctx.Err() != nil {
			return
		}
		owner := repo.Owner.Login
		if owner == "" {
			owner = strings.SplitN(repo.FullName, "/", 2)[0]
		}
		name := repo.Name
		if name == "" {
			name = strings.SplitN(repo.FullName, "/", 2)[1]
		}

		if r.excluded(repo.FullName) {
			if n, err := r.removeOurHooks(ctx, owner, name); err != nil {
				slog.Error("reconcile: remove hook from excluded repo failed", "repo", repo.FullName, "error", err)
			} else if n > 0 {
				changed += n
				slog.Info("reconcile: removed hook from excluded repo", "repo", repo.FullName)
			}
			continue
		}

		if !repo.HasIssues || repo.Mirror || repo.Archived {
			skipped++
			continue
		}

		ok, err := r.ensureHook(ctx, owner, name)
		if err != nil {
			// A private address may be refused by Gitea's webhook host
			// allow-list; that is fixed in gitops on the Gitea deployment.
			slog.Error("reconcile: ensure hook failed", "repo", repo.FullName, "error", err)
			continue
		}
		if ok {
			changed++
			slog.Info("reconcile: hook created or updated", "repo", repo.FullName, "name", hookName, "url", hookURL)
		}
	}
	slog.Info("reconcile pass complete", "repos", len(repos), "changed", changed, "skipped", skipped, "took", time.Since(started))
}

// excluded reports whether a full repository name matches any REPO_EXCLUDE
// glob (for example "grok/*").
func (r *reconciler) excluded(fullName string) bool {
	for _, pattern := range r.cfg.repoExclude {
		if ok, err := path.Match(pattern, fullName); err == nil && ok {
			return true
		}
	}
	return false
}

func (r *reconciler) removeOurHooks(ctx context.Context, owner, repo string) (int, error) {
	hooks, err := r.api.listHooks(ctx, owner, repo)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, h := range hooks {
		if h.url() != hookURL {
			continue
		}
		if err := r.api.deleteHook(ctx, owner, repo, h.ID); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

// ensureHook makes the repository carry exactly one ntfy-issues hook with the
// desired URL, events, secret and active state. It reports whether anything
// was changed.
func (r *reconciler) ensureHook(ctx context.Context, owner, repo string) (bool, error) {
	hooks, err := r.api.listHooks(ctx, owner, repo)
	if err != nil {
		return false, err
	}
	var ours []giteaHook
	for _, h := range hooks {
		if h.url() == hookURL {
			ours = append(ours, h)
		}
	}
	if len(ours) == 0 {
		if err := r.api.createHook(ctx, owner, repo, r.cfg.webhookSecret); err != nil {
			return false, err
		}
		return true, nil
	}
	// Keep the first, remove accidental duplicates.
	changed := false
	sort.Slice(ours, func(i, j int) bool { return ours[i].ID < ours[j].ID })
	for _, extra := range ours[1:] {
		if err := r.api.deleteHook(ctx, owner, repo, extra.ID); err != nil {
			return changed, err
		}
		changed = true
	}
	h := ours[0]
	if hookMatches(&h, r.cfg.webhookSecret) {
		return changed, nil
	}
	if err := r.api.editHook(ctx, owner, repo, h.ID, r.cfg.webhookSecret); err != nil {
		return changed, err
	}
	return true, nil
}

func hookMatches(h *giteaHook, secret string) bool {
	if !h.Active {
		return false
	}
	want := []string{"issues", "issue_comment"}
	got := append([]string(nil), h.Events...)
	sort.Strings(got)
	sort.Strings(want)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		return false
	}
	if h.Config["url"] != hookURL || h.Config["content_type"] != "json" {
		return false
	}
	if secret != "" {
		// Gitea never returns the secret, so it cannot be compared; it is
		// rewritten on every drift we can see. An unchanged hook is left
		// alone rather than patched on every tick.
		if h.Config["secret"] != "" && h.Config["secret"] != secret {
			return false
		}
	}
	return true
}
