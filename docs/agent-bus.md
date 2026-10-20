# Agent-bus messages (private Slack)

This guide owns the integration contract between a firstmate home and an agent bus that relays Slack messages, plus the prerequisites for deploying it.
`bin/fm-procevent-bus.sh` owns exact commands and flags, [`configuration.md`](configuration.md#agent-bus-messages-env-and-statebus) owns the configuration keys and state layout, and the `process-event-sources` skill owns how firstmate handles a delivered message.

## Shape

```
Slack @FirstMate ─▶ Slack bridge (bus identity slack-firstmate)
                      │  POST /api/messages to=firstmate
                      ▼
                  agent bus ◀── GET /api/inbox (cursor, never claims)
                      ▲              │
                      │              ▼
                      │      bin/fm-procevent-bus.sh source (one runner)
                      │              │ captured result + ordinary check wake
                      │              ▼
                      │      main firstmate captain (wake queue, no pane input)
                      │              │
                      └── reply_to=<id>, idem=reply:<id>
                  bridge posts the reply into the original Slack thread
```

The adapter is a process-event source, so a message reaches the captain through the same durable wake queue every other wake uses.
Nothing types into the captain's terminal, so it works the same under Herdr or any other runtime.

## Scope

The first scope is Richard's private Slack with Richard as the only human who can reach firstmate.
It is answer-oriented: firstmate answers from what it already knows or can read and does not start, steer, merge, or change anything on the strength of a bus message alone.
A request for work is acknowledged in the reply and confirmed in the trusted FirstMate session before anything happens.
Other people, workspace-wide policy, and Slack-initiated work are deferred.
The bridge also forwards Richard's unmentioned follow-ups in a thread routed to FirstMate; each arrives as its own bus message and conversation and is answered the same way, with no earlier exchange attached.

## FirstMate side of the wire contract

- Identity: this home holds the bus token for identity `firstmate`.
- Reading: `GET /api/inbox?after=<cursor>&limit=20&wait=<s>` with `Authorization: Bearer <token>`.
  The adapter never calls `/api/claim`, so no lease expires under a busy captain and no message is redelivered by lease timeout.
  The cursor is the highest `created_seq` already captured or refused and is persisted under `state/bus/`.
- One wake per bus message: each captured result carries exactly one `bus_message_id`, and the source never emits an id that an earlier captured result already carries.
  A restart, a lost cursor, or a re-read therefore never produces a second wake for the same message.
- Refusals: a message from a sender outside the allowlist, or with malformed metadata, or from a `slack*` sender without a Slack conversation key, is closed with `POST /api/messages/<id>/ack` `{"ok":false,"error":...,"retry":false}` and logged without its body.
  It never wakes firstmate.
- Reply: `POST /api/messages` `{"reply_to":"<id>","idem":"reply:<id>","body":...}`.
  The broker routes it back to the parent's sender and inherits `conv`, so the reply lands in the same Slack conversation.
  The `reply:<id>` key makes a replayed reply return the first one instead of posting twice.
- Dismissal without a reply: `POST /api/messages/<id>/ack` `{"ok":true,"result":...}`.
- Expiry check: `GET /api/messages/<id>` with the same bearer token, read before replying.

## What the bus and bridge must provide

These are requirements on the agent-hub side; firstmate cannot enforce them.
At agent-hub commit `41a9001877f3eef357a9c19f65150bd76c689f46`, `agent-bus/` and its `deploy/install.sh` provide items 1 to 5, 8, and 9, and item 6 except a generic bearer pattern; the bridge posts no expiry notice, so item 7 is open.

1. A dedicated bridge identity `slack-firstmate`, separate from any other bot's bridge identity, so two bridges never race to claim each other's replies.
2. A broker sender ACL: messages addressed to `firstmate` are accepted only from `slack-firstmate` and `richard`.
   The adapter mirrors this with `FM_BUS_ALLOWED_SENDERS` as defense in depth, but the broker is the boundary.
3. A Slack user allowlist that is non-empty and contains only Richard; the bridge must refuse to start when it is empty.
4. The inbound conversation key `slack:<team>:<channel>:<thread_ts>:<message_ts>:firstmate`, the idempotency key `slack:<channel>:<message_ts>`, and the bridge-written first body line `[Slack mention from Richard (<@U…>) in <#C…>, thread <thread_ts>]`, or `[Slack thread follow-up from …]` for a routed follow-up.
   The bridge appends the bot it fronts to every conversation key, and the adapter refuses a `slack*` sender whose conversation does not end in `:firstmate`, so a conversation built for another bot never wakes firstmate.
   Firstmate treats the whole body, including that line, as untrusted input.
5. Outbound posting only for a reply whose parent came from `slack-firstmate` in the same conversation and an allowlisted channel, with its own Slack metadata event type (for example `firstmate_bus_reply`) for crash-safe duplicate suppression.
6. An outbound secret scrub in the bridge immediately before `chat.postMessage`, covering Slack, Tailscale, Cursor, GitHub, cloud, private-key, and bearer patterns plus every token the bridge's configuration holds.
   The adapter redacts its own token and common credential shapes before posting, but the bridge is the last point that sees the text and knows every configured secret.
7. Expiry notice for messages that expire before firstmate reads them.
   Those never reach firstmate, so the bridge that posted them must tell the Slack user; at `41a9001` it does not, and the Slack thread gets nothing.
   The bridge sets each message's TTL from `SLACK_BUS_TTL_S`, default 3600 seconds, and the broker caps it at `BUS_MAX_TTL_S`, default 86400.
   Set `SLACK_BUS_TTL_S=86400` in `slack-firstmate.env` so expiry means "firstmate was down" rather than "the captain was busy".
   Expiry after capture loses nothing: the broker still accepts the reply and the bridge still posts it in the thread.
8. Authenticated reads: every bus read needs a bearer token, an agent sees only messages it sent or received through `/api/state` and `/api/messages/<id>`, and `/api/events`, `/ws`, and the dashboard's data are admin-only.
   Only the dashboard page and `/healthz`, which carry no message data, are open.
9. Creation caps that bound cursor-inbox consumers: `send_caps` limits the messages each sender creates per rolling 24 hours and all senders together (`install.sh` sets `slack-firstmate` to 100 and `*` to 500; `429` beyond, admin exempt).
   The bridge's `SLACK_THREAD_HOURLY_CAP`, default 20 forwards per thread per hour, still applies; `daily_caps` still covers only Amp wakes.

## Delivery limits

The runner's boundary applies unchanged: a captured message is stored before its wake is published and is re-announced until it is replied to or dismissed.
A message whose bus TTL passes after capture is flagged as expired by `show` and `pending`, and a reply still posts late and reports `parent-expired`.
A message that expires before the source first reads it is lost to firstmate.
The handled acknowledgement does not prove the bus post completed; the `reply:<id>` key is what prevents a duplicate post on replay.
Never describe this path as at-least-once, no-loss, or lossless.

## Deployment prerequisites

Arm the source only in the main home that the always-running captain uses, never in a secondmate home, and only after every item here is settled.

1. Bus side: agent-hub `41a9001` or later is deployed (`~/agent-bus/DEPLOYED` names the commit), the `firstmate` and `slack-firstmate` tokens exist, `slack-firstmate.env` sets `SLACK_ALLOWED_USERS` to Richard's Slack id only and `SLACK_BUS_TTL_S=86400`, and `config.json` and every bridge environment file are mode `0600`.
   Until the bridge posts expiry notices (item 7), accept that a message expiring before capture goes unanswered in Slack.
2. Firstmate code: the VPS checkout must carry the commit that adds this adapter.
   The VPS checkout was installed from upstream `kunchenguid/firstmate`, while this adapter lands on the `lastmanrichard/firstmate` fork, so confirm the live remote and branch first:
   ```sh
   git -C ~/firstmate remote -v
   git -C ~/firstmate branch --show-current
   git -C ~/firstmate status -sb
   crontab -l; systemctl --user list-timers
   ```
3. Update safety: firstmate never updates itself on a timer.
   `/updatefirstmate` runs only when the captain invokes it and fast-forwards the default branch from `origin` only when the checkout is on that branch, clean, and not diverged; otherwise it skips and reports.
   With `origin` still pointing upstream, that update would skip a checkout carrying fork-only commits rather than overwrite them, so the adapter survives but upstream updates stop until the fork and upstream are reconciled.
   Pointing `origin` at the fork makes `/updatefirstmate` deliver whatever lands on the fork's default branch, so only merge reviewed changes there.
   Fleet sync refreshes project clones only, never the firstmate checkout.
4. Credentials: put `FM_BUS_URL` (use the loopback broker URL on the VPS) and `FM_BUS_TOKEN` in the main home's gitignored `.env` at mode `0600`, then run `bin/fm-procevent-bus.sh arm` once.
   A running watcher keeps the source alive across restarts.
5. Accept or remove passwordless sudo.
   Firstmate runs as `richard`, who has passwordless sudo, so a captain manipulated through message content could read the bus admin token, the Slack tokens, and every other secret on the host.
   Separate bus identities do not contain that.
   The mitigations are the Richard-only Slack allowlist, the bridge scrub, the answer-oriented scope above, and removing `NOPASSWD` sudo for `richard`.

## Stopping and rollback

Stop delivery to firstmate with `bin/fm-procevent-bus.sh retire`; captured messages stay readable with `pending` and `show`.
Pausing the broker or stopping the FirstMate bridge also stops new messages without touching other bots.
Rolling back firstmate code means returning the checkout to the previous commit after retiring the source; `state/bus/` and captured results can stay in place.
