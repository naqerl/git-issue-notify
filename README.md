# git-issue-notify

Every new issue and every comment on an issue on `git.naqerl.com` becomes a
tappable push on the phone within seconds, from one piece of configuration
rather than a webhook file in each repository.

The service is a single Go binary (standard library only). It receives Gitea
webhooks at `POST /hook`, maps issue events to ntfy messages, and reconciles the
webhook configuration of every repository on the forge every ten minutes.

```
Gitea pod -- POST /hook --> git-issue-notify -- POST /forge --> ntfy --> phone
     ^                            |
     +-- GET /repos/search, hooks +
```

## What fires

| Gitea event | Condition | ntfy message |
| --- | --- | --- |
| `issues` | `action: opened` | title `new issue · <owner>/<repo>#<n>`, body = issue title + opener + URL, tags `ticket`, priority 4, click opens the issue |
| `issue_comment` | `action: created`, not a pull request | title `comment · <owner>/<repo>#<n>`, body `@<sender>: <first 300 chars>`, tags `speech_balloon`, priority 3, click opens the comment |

Everything else publishes nothing: `edited`, `closed`, `reopened`, `deleted`,
label and assignee changes, and comments on pull requests. Actions whose sender
is in `IGNORE_SENDERS` (default `naqerl`) are acknowledged with `202` and
dropped, so your own comments do not buzz your own phone.

When events arrive faster than the publisher may send them (one message per two
seconds), a burst is folded into one message per repository titled
`N new events on <owner>/<repo>`. Nothing is silently dropped while the bounded
queue has room.

## Endpoint contract

`POST /hook`

- Gitea sends `Content-Type: application/json`.
- `X-Gitea-Event` is `issues` or `issue_comment`.
- `X-Gitea-Delivery` is the delivery ID; it is remembered for 24 h (1024
  entries) so a Gitea retry never double-pushes.
- `X-Gitea-Signature` is the hex HMAC-SHA256 of the raw request body keyed with
  `WEBHOOK_SECRET`. When the secret is set, a wrong or missing signature gets
  `401` and publishes nothing.
- The answer is `202 Accepted` in well under 100 ms regardless of ntfy; a full
  queue answers `503` so Gitea retries later.

`GET /healthz` answers `200 ok`. It is the readiness and liveness probe.

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `NTFY_URL` | `http://ntfy.default.svc.cluster.local` | ntfy base URL, cluster DNS; no auth |
| `NTFY_TOPIC` | `forge` | topic to publish to; `pi` is the whole rollback if `forge` proves redundant |
| `WEBHOOK_SECRET` | empty | shared secret for the hook; also enables HMAC verification |
| `GITEA_URL` | `http://gitea.default.svc.cluster.local:3000` | forge API base URL |
| `GITEA_TOKEN` | empty | site-admin token used by the reconciler; empty disables reconciliation |
| `IGNORE_SENDERS` | `naqerl` | comma-separated logins whose events are dropped |
| `REPO_EXCLUDE` | `grok/*` | comma-separated globs of repositories whose hooks are removed |
| `RECONCILE_INTERVAL` | `10m` | how often every repository is checked |

The Gitea token and the webhook secret are supplied from a sops-encrypted
Secret. Neither appears in this repository, in gitops plaintext, in a workflow
file or in a ConfigMap.

## Webhook reconciliation

On start and every `RECONCILE_INTERVAL` the service pages
`GET /api/v1/repos/search` and makes each eligible repository carry one hook
pointing at `http://git-issue-notify.default.svc.cluster.local/hook`, with
events `issues` and `issue_comment`, the shared secret and `active: true`.
Eligible means: issues enabled, not a mirror, not archived, and not matching
`REPO_EXCLUDE`. Hooks pointing at the service URL on excluded repositories are
deleted. A hook removed by hand therefore reappears within ten minutes; there is
no imperative setup step to drift.

Repository hooks are used rather than one site-wide admin hook because
repository hooks have guaranteed issue-event delivery, and the operator token is
site-admin, so the reconciler can manage them without a settings page.

## Build and release

`.github/workflows/release.yml` runs on a `v*` tag and pushes
`ghcr.io/naqerl/git-issue-notify:<tag>` and `:latest` for `linux/amd64` only,
authenticating with the workflow's own `GITHUB_TOKEN`
(`permissions: packages: write`). No personal access token is stored anywhere.
The package is public: the image carries no secrets.

Deployment lives in the gitops repository at
`clusters/lab/git-issue-notify/`.

See [docs/notifications.md](docs/notifications.md) for what deliberately does
not fire, how to silence a repository, how to mute the phone, and where the path
can break.
