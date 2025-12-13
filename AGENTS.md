# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Test Commands

```bash
# Run all tests (Kaocha)
clj -M:test

# Run specific test namespace
clj -M:test --focus konserve-sync.sync-test

# Watch mode (re-run on file changes)
clj -M:test --watch

# Run ClojureScript tests (shadow-cljs)
npx shadow-cljs compile test
```

## Architecture Overview

konserve-sync is a single-writer replication layer for [konserve](https://github.com/replikativ/konserve) key-value stores. It enables real-time sync from a primary store to multiple subscribers.

### Core Modules

**`sync.cljc`** - Main orchestration layer
- `SyncContext` record holds supervisor, state atom, and options
- Server-side: `register-store!`, `serve-subscription!`, `unregister-store!`
- Client-side: `subscribe!`, `unsubscribe!`, `register-callback!`
- State atom structure: `{:stores {store-id {...}}, :subscriptions {...}, :receivers {...}}`

**`emitter.cljc`** - Server-side change detection
- Hooks into konserve's write-hook system via `k/add-write-hook!`
- `EmitterState` wraps store with update channel and filter function
- Broadcasts changes to the update channel for relay to subscribers

**`receiver.cljc`** - Client-side update application
- `ReceiverState` wraps local store with callback registry
- `apply-update!` writes incoming messages to local konserve store
- `CallbackRegistry` enables per-key update notifications

**`protocol.cljc`** - Message protocol and store identification
- Message types: `:sync/subscribe`, `:sync/update`, `:sync/batch-complete`, etc.
- `store-id` computes UUID from normalized config via hasch
- Critical: `:scope` key must match between client/server for store identity

**`transport/protocol.cljc`** - Transport abstraction
- `PSyncTransport`: `send!`, `on-message!`, `close!`
- `PSyncServer`: `on-connection!`, `broadcast!`

**`transport/channels.cljc`** - In-memory channel transport for testing
**`transport/kabel.cljc`** - Network transport via WebSockets

### Sync Flow

1. Server calls `register-store!` which sets up write-hook emitter
2. Client calls `subscribe!` with local-key-timestamps map
3. Server compares timestamps, sends only newer/missing keys in batches
4. Batch acks provide backpressure flow control
5. After initial sync completes, incremental updates flow via write-hooks

### Key Dependencies

- `superv.async` - Supervision and error handling (`go-try`, `<?`, `S`)
- `konserve` - Key-value store abstraction
- `hasch` - Content-addressed hashing for store-id computation
- `kabel` - Network transport (dev/test dependency)
- `kaocha` - Test runner

### Code Style Notes

- All source files are `.cljc` (cross-platform Clojure/ClojureScript)
- Reader conditionals: `#?(:clj ... :cljs ...)` for platform-specific code
- Async operations use `go-try`/`go-loop-try` with supervisor `S`
- Error propagation via `<?` (throwing take from channel)
