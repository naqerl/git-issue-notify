# Issue notifications: git.naqerl.com → ntfy `forge`

The bridge `git-issue-notify` turns issue activity on the forge into push
notifications. One deployment, one topic, every repository that has issues
enabled.

## What fires

- A **new issue** → `new issue · owner/repo#N`, tags `ticket`, priority 4,
  tapping opens the issue.
- A **new comment on an issue** → `comment · owner/repo#N`, body
  `@author: first 300 characters`, tags `speech_balloon`, priority 3, tapping
  opens the comment.

## What deliberately does not fire

- Issue **edits, closes, reopens, deletes**, label, assignee and milestone
  changes. Only `opened` and `created` are mapped; everything else is noise on a
  phone.
- **Pull request comments.** `issue_comment` payloads whose `issue` carries a
  `pull_request` object are dropped. Review traffic is a different stream and
  would drown issues; a PR stream would be a separate task.
- **Your own actions.** Requests whose `sender.login` is in `IGNORE_SENDERS`
  (default `naqerl`) are acknowledged and published nowhere. Set
  `IGNORE_SENDERS=naqerl,ci-bot` to silence a bot account without a rebuild.
- **Excluded repositories.** Repositories matching `REPO_EXCLUDE` (default
  `grok/*`, the mirror namespace) have the hook removed and never publish. The
  mirrors duplicate `x/*`; without the exclusion every push would arrive twice.

## Silence one repository

Add its glob to `REPO_EXCLUDE` in the deployment (comma-separated, for example
`grok/*,naqerl/noisy-repo`) and let Flux update the pod. On the next reconcile
tick the bridge deletes its own hook from that repository, so the setting is
enforced, not merely ignored.

## Mute the phone

Unsubscribe from the `forge` topic in the ntfy app. Publishing continues; the
phone stops buzzing. Because `forge` carries only issue chatter, muting it does
not mute build or deploy notifications, which stay on `pi`. If `forge` ever
proves redundant, set `NTFY_TOPIC=pi` on the deployment; that is the whole
rollback.

## Where the path can break

| Symptom | Cause | What happens next |
| --- | --- | --- |
| A repository stops notifying | Its hook was deleted by hand | The reconciler recreates it within `RECONCILE_INTERVAL` (10 min) |
| No pushes at all for minutes | ntfy is unreachable | The bridge still answers Gitea `202`; the event is retried in-process with backoff for five minutes, then Gitea's own hook retry takes over |
| Hook creation fails with `422`/host not allowed | Gitea refuses a cluster-internal webhook URL | Set `GITEA__webhook__ALLOWED_HOST_LIST="*"` on the Gitea deployment in gitops and reconcile |
| A burst produces fewer messages than events | Coalescing | One message per repository titled `N new events on owner/repo`; the count is deliberate and never a silent drop |
| A Gitea retry does not produce a second push | Delivery dedupe | Intended: at most once wins over completeness |

## Prove it

On the live forge, open a scratch issue from an account other than `naqerl` and
subscribe to `forge`: exactly one `new issue · …` push arrives. Comment on it:
one `comment · …` push. Comment on a pull request: none. Comment as `naqerl`:
none. Flapping the hook by hand and waiting for the tick brings it back.
