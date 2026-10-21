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
                      │      main firstmate session (wake queue, no pane input)
                      │              │
                      └── reply_to=<id>, idem=reply:<id>
                  bridge posts the reply into the original Slack thread
```

The adapter is a process-event source, so a message reaches firstmate through the same durable wake queue every other wake uses.
Nothing is typed into firstmate's terminal, so it works the same under any runtime backend.

## Scope

Only senders on the allowlist reach firstmate, and the bridge decides which Slack users may reach the bus.
A message body is untrusted input and carries no authority of its own.
The adapter delivers messages and returns replies; it never starts work on a message's behalf.
Each Slack mention, and each follow-up the bridge forwards from a thread routed to FirstMate, arrives as its own bus message and conversation with no earlier exchange attached.

## FirstMate side of the wire contract

- Identity: this home holds the bus token for identity `firstmate`.
- Reading: `GET /api/inbox?after=<cursor>&limit=20&wait=<s>` with `Authorization: Bearer <token>`.
  The adapter never calls `/api/claim`, so no lease expires while firstmate is busy and no message is redelivered by lease timeout.
  The cursor is the highest `created_seq` already captured or refused and is persisted under `state/bus/`.
- One wake per bus message: each captured result carries exactly one `bus_message_id`, and the source never emits an id that an earlier captured result already carries.
  A restart, a lost cursor, or a re-read therefore never produces a second wake for the same message.
- Refusals: a message from a sender outside the allowlist, with malformed metadata, or from a `slack*` sender whose conversation key does not end in `:firstmate` is closed with `POST /api/messages/<id>/ack` `{"ok":false,"error":...,"retry":false}` and logged without its body.
  It never wakes firstmate.
- Reply: `POST /api/messages` `{"reply_to":"<id>","idem":"reply:<id>","body":...}`.
  The broker routes it back to the parent's sender and inherits `conv`, so the reply lands in the same Slack conversation.
  The `reply:<id>` key makes a replayed reply return the first one instead of posting twice.
- Dismissal without a reply: `POST /api/messages/<id>/ack` `{"ok":true,"result":...}`.
- Expiry check: `GET /api/messages/<id>` with the same bearer token, read before replying.

## What the bus and bridge must provide

These are requirements on the bus side; firstmate cannot enforce them.

1. A dedicated bridge identity `slack-firstmate`, separate from any other bot's bridge identity, so two bridges never race to claim each other's replies.
2. A broker sender ACL: messages addressed to `firstmate` are accepted only from `slack-firstmate` and the operator identity `richard`.
   The adapter mirrors this with `FM_BUS_ALLOWED_SENDERS` as defense in depth, but the broker is the boundary.
   Its default, `slack-firstmate richard`, suits a private single-operator deployment; set `FM_BUS_ALLOWED_SENDERS` in `.env` to change it, keeping it no wider than the broker ACL.
3. A non-empty Slack user allowlist; the bridge must refuse to start when it is empty.
4. The inbound conversation key `slack:<team>:<channel>:<thread_ts>:<message_ts>:firstmate` and the idempotency key `slack:<channel>:<message_ts>`.
   The adapter refuses a `slack*` sender whose conversation does not end in `:firstmate`, so a conversation built for another bot never wakes firstmate.
5. Outbound posting only for a reply whose parent came from `slack-firstmate` in the same conversation and an allowlisted channel, with its own Slack metadata event type for duplicate suppression.
6. An outbound secret scrub in the bridge immediately before posting to Slack, covering every token the bridge's configuration holds.
   The adapter redacts its own token and common credential shapes before posting, but the bridge is the last point that sees the text.
7. Expiry notice for messages that expire before firstmate reads them; those never reach firstmate, so the bridge that posted them must tell the Slack user.
   Expiry after capture loses nothing: the broker still accepts the reply and the bridge still posts it.
8. Authenticated reads: every bus read needs a bearer token, and an agent sees only messages it sent or received.
9. Per-sender creation caps, so a cursor-inbox consumer is never flooded.

## Not supported: messages firstmate starts

The adapter only receives messages and answers them; it has no command that starts a conversation, so firstmate cannot send a request to another agent, such as Puck, through it.
Do not add another agent's identity to `FM_BUS_ALLOWED_SENDERS` to work around this: `reply` answers through `reply_to`, so replying to an agent's message addresses that agent again and can loop until the broker's hop limit.

## Delivery limits

The runner's boundary applies unchanged: a captured message is stored before its wake is published and is re-announced until it is replied to or dismissed.
A message whose bus TTL passes after capture is flagged as expired by `show` and `pending`, and a reply still posts late and reports `parent-expired`.
A message that expires before the source first reads it is lost to firstmate.
The handled acknowledgement does not prove the bus post completed; the `reply:<id>` key is what prevents a duplicate post on replay.
Never describe this path as at-least-once, no-loss, or lossless.

## Deployment

Arm the source only in the main home, never in a secondmate home, and only after the bus provides the requirements above.

1. Put `FM_BUS_URL` and `FM_BUS_TOKEN` in the main home's gitignored `.env` at mode `0600`.
2. Run `bin/fm-procevent-bus.sh arm` once; a running watcher keeps the source alive across restarts.
3. Firstmate's host privileges bound what a manipulated message could reach, so run it with no more access to the host's secrets than it needs.

## Stopping and rollback

Stop delivery with `bin/fm-procevent-bus.sh retire`; captured messages stay readable with `pending` and `show`.
Rolling back firstmate code means retiring the source first; `state/bus/` and captured results can stay in place.
