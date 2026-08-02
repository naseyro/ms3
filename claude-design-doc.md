# Lightweight S3-Compatible Object Storage — Design Document

## 1. Goal & Scope

Build a minimal, MinIO-inspired object storage server: S3-compatible REST API, a CLI client, pluggable storage backends (local disk, WebDAV, extensible), any MIME type, presigned URLs with expiration, and a Dockerized deployment. Optimized for **simplicity and learnability over feature completeness** — you're trading erasure coding, distributed consensus, and multi-node clustering (MinIO's hardest parts) for a single-node, well-architected system that still speaks real S3 protocol.

---

## 2. High-Level Architecture

```
                    ┌─────────────────────┐
   aws-cli / SDKs   │                     │
   your CLI (msc) ──►   REST API Server   │
   presigned links  │   (Go, net/http)    │
                    └──────────┬──────────┘
                               │
                ┌──────────────┼──────────────┐
                ▼                             ▼
      ┌──────────────────┐         ┌──────────────────────┐
      │  Metadata Store   │         │   Storage Backend      │
      │  (SQLite/Postgres)│         │   interface             │
      │  users, buckets,   │        │  ┌─────────┐┌─────────┐ │
      │  objects, ACLs     │        │  │Local FS ││ WebDAV  │ │
      └──────────────────┘         │  └─────────┘└─────────┘ │
                                    └──────────────────────┘
```

Two independent stores, one for **metadata** (structured, queryable) and one for **bytes** (dumb, streamable). This split is exactly how MinIO, S3, and most object stores are conceptually organized — don't try to store blobs in the DB.

---

## 3. Answering Your Three Questions First

### Q1: How to distribute buckets for different users — using a DB?

Yes. The DB owns identity and namespace; the filesystem only owns bytes. Suggested schema:

```sql
-- Users / access credentials
CREATE TABLE users (
    id            TEXT PRIMARY KEY,       -- uuid
    username      TEXT UNIQUE NOT NULL,
    access_key    TEXT UNIQUE NOT NULL,   -- like AWS access key id
    secret_key    TEXT NOT NULL,          -- encrypted at rest (see 3.2)
    is_admin      BOOLEAN DEFAULT FALSE,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Buckets (global namespace, like real S3)
CREATE TABLE buckets (
    id            TEXT PRIMARY KEY,
    name          TEXT UNIQUE NOT NULL,
    owner_id      TEXT NOT NULL REFERENCES users(id),
    versioning    BOOLEAN DEFAULT FALSE,
    public_read   BOOLEAN DEFAULT FALSE,  -- simple bucket policy, optional
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Objects (one row per key, or per version if you add versioning later)
CREATE TABLE objects (
    id            TEXT PRIMARY KEY,
    bucket_id     TEXT NOT NULL REFERENCES buckets(id),
    object_key    TEXT NOT NULL,          -- e.g. "photos/2024/img.png"
    size_bytes    BIGINT NOT NULL,
    etag          TEXT NOT NULL,          -- MD5 of content
    content_type  TEXT,
    storage_ref   TEXT NOT NULL,          -- path/pointer into the backend
    version_id    TEXT DEFAULT 'null',
    is_latest     BOOLEAN DEFAULT TRUE,
    deleted_at    TIMESTAMP,              -- soft delete
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(bucket_id, object_key, version_id)
);
CREATE INDEX idx_objects_bucket_key ON objects(bucket_id, object_key);
```

**Bucket naming**: make bucket names globally unique across the whole system (not per-user), same as real S3. This matters because S3 clients address buckets via virtual-host (`bucket.yourdomain.com`) or path style (`/bucket/key`) — there's no "user scope" in the protocol itself. A user's *ownership* is an authorization concept sitting on top, not a namespace concept.

**DB choice**: since you explicitly want "lightweight," start with **SQLite**. It's a single file, zero ops overhead, and Go's `database/sql` + `mattn/go-sqlite3` (or pure-Go `modernc.org/sqlite` to avoid cgo) make this trivial. Define a `MetadataStore` interface so you can swap in Postgres later for concurrent multi-instance deployments without touching your handlers:

```go
type MetadataStore interface {
    CreateBucket(ctx context.Context, b Bucket) error
    GetBucket(ctx context.Context, name string) (Bucket, error)
    PutObject(ctx context.Context, o Object) error
    GetObject(ctx context.Context, bucket, key string) (Object, error)
    ListObjects(ctx context.Context, bucket, prefix string) ([]Object, error)
    DeleteObject(ctx context.Context, bucket, key string) error
    // ...
}
```

SQLite's one real limitation: it locks the whole DB file on writes, so it's fine for a single-instance server but won't scale to multiple app replicas behind a load balancer. That's a good natural point to graduate to Postgres — same interface, new implementation.

### Q2: Where to store configuration?

Split into two clearly different kinds of "config," because conflating them is the most common mistake here:

| Kind | Examples | Where it lives |
|---|---|---|
| **Static app config** | server port, storage backend type (local/webdav), local root path or WebDAV URL, DB connection string, default presign expiry, TLS cert paths | YAML/TOML file + env var overrides |
| **Dynamic state** | users, buckets, objects, ACLs | The metadata DB (never a config file) |

For static config, use a layered approach (defaults → config file → env vars, in increasing priority — the standard 12-factor pattern):

```yaml
# config.yaml
server:
  port: 9000
  presign_default_expiry: 3600s
  presign_max_expiry: 7d

storage:
  backend: local          # local | webdav
  local:
    root_path: /data
  webdav:
    url: https://webdav.example.com/store
    username: ""
    password: ""

metadata:
  driver: sqlite          # sqlite | postgres
  dsn: /data/meta.db
```

Load with `spf13/viper` (handles YAML + env override in one call) or keep it dependency-light with plain `encoding/yaml` + `os.Getenv` fallbacks — either is fine for this scope.

**Secrets nuance worth knowing**: S3's auth scheme (SigV4) is an HMAC signature computed from the shared secret key — the server must be able to reconstruct the exact same signature the client sent. That means **you cannot one-way-hash the secret key like a password** (bcrypt etc.) — the server needs the plaintext-equivalent secret to verify signatures. So encrypt secret keys at rest with an application-level master key (from an env var, e.g. `MASTER_ENCRYPTION_KEY`, using AES-GCM), rather than storing them in plain text in the DB. This is a real, common gotcha people miss when building S3-compatible auth.

### Q3: How to write the object storage algorithm?

There isn't a single "algorithm" so much as a **storage backend interface** plus a few concrete decisions. Define:

```go
type Backend interface {
    Put(ctx context.Context, ref string, r io.Reader) (size int64, md5sum string, err error)
    Get(ctx context.Context, ref string) (io.ReadCloser, error)
    Delete(ctx context.Context, ref string) error
    Stat(ctx context.Context, ref string) (Info, error)
}
```

This shape maps cleanly onto **both** local filesystem and WebDAV, because WebDAV is literally a network filesystem protocol (PUT/GET/DELETE/PROPFIND map almost 1:1 onto this interface). That's why your instinct to abstract "local FS or WebDAV" behind one interface is exactly right.

Key design decisions inside the algorithm:

**a) Key-to-path mapping — two options**

1. **Direct mapping** (recommended for v1): `storage_ref = <bucket>/<object_key>`, so `photos/img.png` in bucket `my-app` becomes `/data/my-app/photos/img.png` on disk. Simple, human-browsable, trivial to reason about, and works identically over WebDAV. Downsides: no dedup, and you must sanitize keys against path traversal (`../../etc/passwd`) — always resolve and check the final path is still inside the bucket root.

2. **Content-addressed storage (CAS)**: hash the content (SHA-256) and store at `/data/objects/ab/cd/abcd1234...`, keeping the key→hash mapping only in the DB (like git's object store, or how many dedup-capable stores work). Gives you free deduplication and immutability, but complicates the WebDAV backend (now WebDAV also needs the hash-bucket convention) and complicates deletion (can't delete a blob if another key still references it — needs refcounting). **Worth doing as a v2 enhancement**, not v1.

**b) Streaming, not buffering**

Never read the whole upload into memory. Stream directly from the HTTP request body into the backend, computing the MD5 (for the ETag) as you go using `io.MultiWriter`:

```go
hasher := md5.New()
file, _ := os.CreateTemp(bucketDir, "upload-*.tmp")
written, err := io.Copy(io.MultiWriter(file, hasher), r.Body)
```

**c) Atomic writes**

Write to a temp file, `fsync`, then `os.Rename` into place. Rename is atomic on the same filesystem, so a crash mid-upload never leaves a half-written object visible under its real key. This is the single most important correctness detail in the whole system.

**d) Write ordering between DB and disk**

You have two stores that need to agree. Recommended ordering:
- **On upload**: write bytes to disk first (temp → rename) → then insert/update the DB row pointing at it. If the DB write fails after the file lands, you get an orphaned file (harmless, cleaned up later by a GC sweep) rather than a DB row pointing at nothing.
- **On delete**: soft-delete the DB row first (mark `deleted_at`) → then delete the file async. If the file delete fails/crashes, a background job can retry — but the DB is already source of truth for whether the object is "gone" from the API's perspective.

**e) Background GC goroutine**

Since you're deep in goroutines/channels right now, this is a natural place to apply them: a ticker-driven background worker that periodically (a) removes orphaned temp files older than N minutes, and (b) hard-deletes soft-deleted rows/files older than a retention window. A single `time.Ticker` + goroutine + `context.Context` for shutdown is all you need — no need for anything fancier.

---

## 4. REST API Surface (S3-Compatible Subset)

| Operation | Method & Path | Notes |
|---|---|---|
| Create bucket | `PUT /{bucket}` | |
| Delete bucket | `DELETE /{bucket}` | only if empty |
| List buckets | `GET /` | owner-scoped |
| List objects | `GET /{bucket}?list-type=2&prefix=...` | S3's ListObjectsV2 shape |
| Put object | `PUT /{bucket}/{key}` | body = raw bytes |
| Get object | `GET /{bucket}/{key}` | supports `Range` header |
| Head object | `HEAD /{bucket}/{key}` | metadata only |
| Delete object | `DELETE /{bucket}/{key}` | |
| Presigned GET | `GET /{bucket}/{key}?X-Amz-Signature=...&X-Amz-Expires=...` | see §6 |

**Auth — the part that makes it "S3-compatible" rather than "S3-like"**: implementing **AWS SigV4** request signing/verification is what lets existing tools (`aws-cli`, `boto3`, any S3 SDK) work against your server unmodified. It's a well-documented algorithm (canonical request → string-to-sign → HMAC chain) and MinIO's own server source is a legitimate reference implementation to read. It's real work, so treat it as a distinct milestone rather than bolting it on last.

If you want to de-risk the project, you can ship a v1 with a simplified custom header-based auth (e.g., `Authorization: MSC <access-key>:<hmac-signature>`) and layer in full SigV4 compatibility once the core engine works — you lose "drop-in aws-cli compatibility" temporarily but keep momentum.

---

## 5. CLI Client

Model it directly on `mc`. Use `spf13/cobra` (industry standard for Go CLIs, used by kubectl, gh, etc.):

```
msc mb myserver/my-bucket          # make bucket
msc ls myserver/my-bucket          # list objects
msc cp ./photo.png myserver/my-bucket/photo.png
msc cat myserver/my-bucket/photo.png
msc rm myserver/my-bucket/photo.png
msc presign myserver/my-bucket/photo.png --expiry 1h
```

The CLI is just a thin client that signs requests the same way your server verifies them — building both sides forces you to get the signing scheme exactly right.

---

## 6. Presigned URLs

Since you're implementing SigV4-style signing anyway, presigned URLs fall out almost for free — they're the *same* signature algorithm, just with the signature and expiry passed as **query parameters** instead of an `Authorization` header, so they work as clickable links.

Generation (CLI or server-side):
1. Build the canonical request as usual, but put `X-Amz-Expires`, `X-Amz-Date`, `X-Amz-SignedHeaders` in the query string.
2. Sign it with the requester's secret key.
3. Append `X-Amz-Signature` to the URL.

Verification (server middleware on every request):
1. Recompute the same canonical request/signature from the incoming query params.
2. Compare signatures (constant-time compare).
3. Reject if `now > X-Amz-Date + X-Amz-Expires`.

This needs **no extra DB table** — it's fully stateless/self-contained in the URL, which keeps it "lightweight" and also means presigned URLs work correctly even across restarts. (Trade-off: you can't *revoke* an individual presigned URL early without a revocation list — acceptable for v1, worth a note in the doc.)

---

## 7. Suggested Go Project Layout

```
/cmd
  /server        # main.go for the API server
  /msc           # main.go for the CLI client
/internal
  /api           # HTTP handlers, routing
  /auth          # SigV4 sign/verify
  /presign       # presigned URL generation/verification
  /storage       # Backend interface + local/webdav implementations
  /metadata      # MetadataStore interface + sqlite/postgres implementations
  /config        # config loading (yaml + env)
  /gc            # background cleanup goroutine
/pkg
  /s3types       # shared request/response structs
Dockerfile
docker-compose.yml
config.yaml
```

This mirrors your existing microservices instincts (interfaces at package boundaries, swappable implementations) — same shape you already use for things like your analytics engine's pluggable design, just applied to storage backends and metadata stores instead of Redis Streams.

---

## 8. Docker & Deployment

```yaml
# docker-compose.yml
services:
  storage-server:
    build: .
    ports:
      - "9000:9000"
    volumes:
      - object-data:/data          # local backend root
      - ./config.yaml:/app/config.yaml
    environment:
      - MASTER_ENCRYPTION_KEY=${MASTER_ENCRYPTION_KEY}
    # optional, only if you graduate to postgres:
    # depends_on:
    #   - postgres

  # postgres:
  #   image: postgres:16
  #   environment:
  #     POSTGRES_DB: objstore
  #     POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
  #   volumes:
  #     - pg-data:/var/lib/postgresql/data

volumes:
  object-data:
  # pg-data:
```

With SQLite as the default metadata store, your entire system is a **single container + one volume** — genuinely lightweight to run.

---

## 9. Additional Features Worth Considering

- `/healthz` liveness/readiness endpoint
- Request size limits + basic rate limiting (protects against accidental abuse)
- Multipart upload (`CreateMultipartUpload` / `UploadPart` / `CompleteMultipartUpload`) — needed for large files, but genuinely a phase-2 item; skip for MVP
- CORS support if you ever want browser-based uploads
- Bucket quotas (max size/object count per bucket)
- Object versioning (soft-delete + `is_latest` flag is already schema-ready for this)
- Given your OpenTelemetry background: instrumenting the server with OTel traces/metrics would be a natural, low-effort addition and would let you reuse skills you already have — spans around `Put`/`Get` backend calls, a counter for bytes stored, etc.

---

## 10. Suggested Build Order (Phased Roadmap)

**Phase 1 — MVP**
- Local filesystem backend only
- SQLite metadata store
- Core object operations: put/get/delete/list bucket & object
- Simple custom auth (access key + HMAC header) — not yet full SigV4
- Presigned URLs using the same simplified signing scheme
- CLI with `mb`, `ls`, `cp`, `rm`, `presign`
- Single-container Dockerfile

**Phase 2 — S3 Compatibility**
- Full AWS SigV4 request signing/verification (so `aws-cli`/SDKs work unmodified)
- WebDAV backend implementation
- Multipart upload support
- docker-compose with optional Postgres metadata store

**Phase 3 — Polish**
- Object versioning
- Bucket policies (public-read, etc.)
- Quotas
- OTel tracing/metrics
- Simple web UI (optional)

---

## 11. Key Trade-offs You're Explicitly Making

- **No erasure coding / replication** — single copy of data, single point of failure. Acceptable for a lightweight/dev-focused tool; document it clearly so nobody mistakes this for production-durable storage.
- **No multi-node clustering** — one server instance (SQLite ties you to this anyway until you swap to Postgres).
- **Presigned URLs are non-revocable** before expiry (stateless design) — fine for the stated use case, worth flagging as a known limitation.

This gives you a system that's genuinely learnable end-to-end in a way MinIO's actual codebase isn't, while still being real S3-protocol-compatible once you get to Phase 2's SigV4 work.
