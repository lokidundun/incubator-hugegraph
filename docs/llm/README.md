# Repo Memory (Cross-Repo Level)

This directory is the repo-level memory hub for maintainers and AI tools.
It adapts LLM Wiki / Gbrain style knowledge to HugeGraph modules and ops.

## Start Here

| File | Purpose | Audience |
|------|---------|----------|
| [llms.txt](llms.txt) | Machine-readable doc index — LLM ingestion entry point | AI tools, LLMs |
| [onboarding.md](onboarding.md) | New contributor guide — First Day Checklist, Module Navigator | Human contributors |
| [code-graph.md](code-graph.md) | Module dependency graph, interface map, data flow | All |
| [repo-adaptation.md](repo-adaptation.md) | Module boundaries, ecosystem repo map, knowledge routing | All |
| [opsx-openspec.md](opsx-openspec.md) | Config registry, memory health cadence, cross-repo sync rules | Maintainers, CI |

## Memory System Integration

This repo is designed to be registered into the [HugeGraph-AI Memory System](https://github.com/apache/hugegraph-ai).

### Quick Start

```bash
# 1. Register this repo into the Memory engine
./scripts/memory-register.sh http://localhost:8000 incubator-hugegraph

# 2. Trigger an incremental update
curl -X POST http://localhost:8000/memory/update \
  -H "Content-Type: application/json" \
  -d '{"repo_tag": "incubator-hugegraph"}'

# 3. Check memory health
curl http://localhost:8000/memory/repos/incubator-hugegraph
```

### Automation

- **Git Hook**: `scripts/hooks/post-commit-memory` — auto-trigger on commit
- **CI Job**: `.github/workflows/memory-sync.yml` — drift check on push/PR

## External References

- LLM Wiki V1 (Graphify): https://github.com/safishamsi/graphify
- Gbrain concept: brain + source dual-axis routing
- Code Graph reference: https://github.com/tirth8205/code-review-graph

## Maintenance Cadence

- **Per-PR**: Git Hook triggers incremental update for changed files.
- **Per-release**: Full re-index via `memory-register.sh` with `force=true`.
- **Quarterly**: Health check via `memory doctor` CLI.
- Update docs/llm files after module boundary changes or new subsystem landing.

## Contribution

Keep notes concise, factual, and link to source code or docs.
When adding new files to this directory, update the table above and `llms.txt`.
