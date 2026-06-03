# Opsx / OpenSpec Conventions

This page describes operational memory and spec conventions for HugeGraph.

## Opsx Targets

- Release packaging: `install-dist/`
- Runtime configs: `hugegraph-server/hugegraph-dist/src/assembly/static/conf/`
- Service scripts: `hugegraph-server/hugegraph-dist/src/assembly/static/bin/`
- PD configs: `hugegraph-pd/hg-pd-dist/src/assembly/static/conf/`
- Store configs: `hugegraph-store/hg-store-dist/src/assembly/static/conf/`

## Spec Practices

- Config keys should be documented in module `README.md` or `docs/`.
- New public APIs should add or update examples.
- Release artifacts must update `install-dist/release-docs/` metadata.

## Ops Memory Maintenance

- When adding new flags, capture defaults and compatibility notes here.
- When changing scripts, add a short note and link to the commit/PR.

---

## Config Spec Registry

Structured index of key configuration items across modules.

### Server Config (`hugegraph.properties`)

| Key | Default | Scope | Notes |
|-----|---------|-------|-------|
| `backend` | `rocksdb` | Storage | Backend type: rocksdb, hstore, mysql, etc. |
| `serializer` | `binary` | Storage | Data serialization format |
| `graphs` | (path) | Multi-graph | Graph config directory |
| `server.id` | `server-1` | Cluster | Unique server identifier |
| `rest-server.port` | `8080` | Network | REST API port |
| `gremlin.server.port` | `8182` | Network | Gremlin server port |

### PD Config (`application.yml`)

| Key | Default | Scope | Notes |
|-----|---------|-------|-------|
| `grpc.port` | `8686` | Network | gRPC service port |
| `raft.address` | `127.0.0.1:8610` | Cluster | This node's Raft address |
| `raft.peers-list` | (self) | Cluster | Comma-separated peer list |
| `pd.data-path` | `./pd_data` | Storage | RocksDB metadata path |
| `partition.default-shard-count` | `1` | Partition | Replicas per partition |

### Store Config (`application.yml`)

| Key | Default | Scope | Notes |
|-----|---------|-------|-------|
| `grpc.port` | `8500` | Network | Store gRPC port |
| `rocksdb.data-path` | (auto) | Storage | RocksDB data directory |
| `raft.election-timeout` | (auto) | Cluster | Raft election timeout |

---

## Memory Health Cadence

Define when and how repository memory should be refreshed.

### Trigger-Based Updates

| Event | Action | Responsibility |
|-------|--------|---------------|
| Module boundary change (new/removed module) | Update `code-graph.md` + `repo-adaptation.md` | PR author |
| New public API added | Update `llms.txt` key paths section | PR author |
| Backend interface change | Update `code-graph.md` Interface Map | PR author |
| gRPC proto change | Update `code-graph.md` gRPC Service Graph | PR author |
| New config key added | Update Config Spec Registry above | PR author |

### Periodic Refresh

| Cadence | Scope | Method |
|---------|-------|--------|
| Per-PR | Changed files only | Git Hook (`post-commit-memory`) |
| Per-release | Full repo | `scripts/memory-register.sh` with `force=true` |
| Quarterly | All registered repos | `memory doctor` CLI health check |

### Health Check Criteria

A repo's memory is considered healthy when:
1. **File drift < 5%**: Files on disk vs files in manifest
2. **No stale modules**: All pom.xml modules accounted for
3. **docs/llm complete**: All required files present and non-empty
4. **AGENTS.md coverage**: Every top-level module has an AGENTS.md

---

## Cross-Repo Sync Rules

When changes in one repository affect another:

| Source Repo | Affected Repo | Sync Rule |
|-------------|--------------|----------|
| incubator-hugegraph (core interface change) | hugegraph-ai | Re-register affected repos |
| incubator-hugegraph (version bump) | All ecosystem repos | Update version matrix in repo-adaptation.md |
| hugegraph-ai (Memory System change) | This repo | Update Memory System section in llms.txt |
| hugegraph-toolchain (client API change) | hugegraph-ai | Re-register toolchain in Memory |

### Memory Registration Commands

```bash
# Register a new repo
./scripts/memory-register.sh http://localhost:8000 incubator-hugegraph

# Trigger incremental update
curl -X POST http://localhost:8000/memory/update \
  -H "Content-Type: application/json" \
  -d '{"repo_tag": "incubator-hugegraph"}'

# Check memory health
curl http://localhost:8000/memory/repos/incubator-hugegraph

# Cross-repo query
curl -X POST http://localhost:8000/memory/query \
  -H "Content-Type: application/json" \
  -d '{"query": "How does authentication work?", "repo_tags": ["incubator-hugegraph"]}'
```
