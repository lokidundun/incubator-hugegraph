# HugeGraph Onboarding Guide

This guide helps new contributors get productive with the HugeGraph codebase quickly.

---

## 1. First Day Checklist

### Environment Setup

```bash
# Verify prerequisites
java -version    # Must be 11+
mvn -version     # Must be 3.5+
git --version

# Clone and build
git clone https://github.com/apache/incubator-hugegraph.git
cd incubator-hugegraph
mvn clean install -DskipTests    # ~5-10 min first build

# Verify build output
ls install-dist/target/hugegraph-*.tar.gz
```

### First Run

```bash
# Extract distribution
tar xzf install-dist/target/hugegraph-*.tar.gz
cd hugegraph-*/

# Initialize backend (RocksDB)
bin/init-store.sh

# Start server
bin/start-hugegraph.sh

# Verify: REST API on :8080, Gremlin on :8182
curl http://localhost:8080/apis/version
```

### First Test

```bash
# Quick unit test (memory backend, fast)
mvn test -pl hugegraph-server/hugegraph-test -am -P unit-test

# Core test with RocksDB (more realistic)
mvn test -pl hugegraph-server/hugegraph-test -am -P core-test,rocksdb
```

---

## 2. Module Navigator

Pick your entry point based on what you want to work on:

### "I want to work on the graph engine"

Start here: `hugegraph-server/hugegraph-core/`

Key packages:
- `org.apache.hugegraph.HugeGraph` — main graph interface
- `org.apache.hugegraph.StandardHugeGraph` — core implementation
- `org.apache.hugegraph.backend.store.BackendStore` — storage abstraction
- `org.apache.hugegraph.schema` — VertexLabel, EdgeLabel, PropertyKey, IndexLabel
- `org.apache.hugegraph.traversal` — Gremlin traversal optimization strategies
- `org.apache.hugegraph.structure` — Vertex, Edge, Property implementations

### "I want to add a REST API"

Start here: `hugegraph-server/hugegraph-api/`

Key packages:
- `org.apache.hugegraph.api.graph.GraphAPI` — graph CRUD operations
- `org.apache.hugegraph.api.schema` — schema management APIs
- `org.apache.hugegraph.api.gremlin.GremlinAPI` — Gremlin query endpoint
- `org.apache.hugegraph.api.cypher.CypherAPI` — Cypher query endpoint

Pattern: Extend `ApiBase`, use JAX-RS annotations (`@Path`, `@GET`, `@POST`).

### "I want to work on distributed features"

Start here: `hugegraph-pd/` and `hugegraph-store/`

**Important**: Build `hugegraph-struct` first:
```bash
mvn install -pl hugegraph-struct -am -DskipTests
```

Key concepts:
- **PD (Placement Driver)**: Metadata coordination, partition management, Raft consensus
- **Store**: Distributed storage with RocksDB + Raft replication
- **gRPC**: All inter-service communication uses Protocol Buffers

### "I want to add a new storage backend"

Start here: `hugegraph-server/hugegraph-rocksdb/` (reference implementation)

Steps:
1. Create new module: `hugegraph-{backend-name}/`
2. Add dependency on `hugegraph-core`
3. Implement `BackendStore` interface
4. Implement `BackendStoreProvider` for factory
5. Add module to parent `pom.xml`

### "I want to work on AI/LLM integration"

Go to the separate repository: `hugegraph-ai/`

Components:
- `hugegraph-llm`: LLM integration, Graph RAG, Memory System
- `hugegraph-ml`: Machine learning algorithms
- `hugegraph-python-client`: Python SDK

---

## 3. Common Task Recipes

### Adding a New REST API Endpoint

```
1. Create API class in hugegraph-api/src/main/java/org/apache/hugegraph/api/
2. Extend ApiBase or relevant base class
3. Use JAX-RS annotations (@Path, @GET, @POST, etc.)
4. Add Swagger/OpenAPI annotations for documentation
5. Add tests in hugegraph-test/src/test/java/.../api/
6. Run: mvn test -pl hugegraph-server/hugegraph-test -am -P api-test,memory
```

### Modifying Schema or Graph Elements

```
1. Schema definitions: hugegraph-core/src/main/java/org/apache/hugegraph/schema/
2. Graph structure: hugegraph-core/src/main/java/org/apache/hugegraph/structure/
3. Consider backward compatibility for stored data
4. Update serializers if changing storage format
5. Run: mvn test -pl hugegraph-server/hugegraph-test -am -P core-test,rocksdb
```

### Changing gRPC Protocol

```
1. Edit .proto files in hg-pd-grpc/ or hg-store-grpc/
2. Run: mvn clean compile (regenerates Java stubs)
3. Generated code: target/generated-sources/protobuf/
4. Update service implementations
5. Update client code if interface changed
6. Run: mvn test -pl hugegraph-pd/hg-pd-test -am
```

### Adding a Third-Party Dependency

```
1. Add license file to install-dist/release-docs/licenses/
2. Declare dependency in install-dist/release-docs/LICENSE
3. Append NOTICE info to install-dist/release-docs/NOTICE (if upstream has NOTICE)
4. Update install-dist/scripts/dependency/known-dependencies.txt
5. Run: mvn apache-rat:check (verify license compliance)
```

---

## 4. Architecture Decision Records

### ADR-001: Pluggable Backend Architecture
- **Decision**: Storage backends implement the `BackendStore` interface
- **Rationale**: Allows adding new backends without modifying core code
- **Trade-off**: Slight performance overhead from abstraction layer

### ADR-002: TinkerPop Compliance
- **Decision**: Full Apache TinkerPop 3 implementation
- **Rationale**: Standard graph query language, ecosystem compatibility
- **Trade-off**: Constrained by TinkerPop's design patterns

### ADR-003: gRPC for Distributed Communication
- **Decision**: All inter-service communication uses gRPC
- **Rationale**: High performance, cross-language support, Protocol Buffers
- **Trade-off**: Requires proto file management and code generation

### ADR-004: Property-Based Repo Isolation (Memory System)
- **Decision**: Use `repo_tag` property on vertices/edges for repo isolation
- **Alternatives considered**: Graphspace isolation, prefix-based
- **Rationale**: Compatible with all HugeGraph versions, enables cross-repo queries

### ADR-005: Dual-Layer Manifest (Memory System)
- **Decision**: JSON file as source of truth + HugeGraph vertex as queryable mirror
- **Rationale**: Fast offline change detection + Gremlin queryability
- **Trade-off**: Requires keeping two data sources in sync

---

## 5. FAQ & Pitfalls

### Build fails with "Could not resolve dependencies"
Make sure you build from the root directory first. Some modules depend on others:
```bash
mvn install -pl hugegraph-struct -am -DskipTests  # Build struct first for PD/Store
mvn clean install -DskipTests                       # Then full build
```

### Tests fail with "Backend not initialized"
Use the `memory` profile for tests that don't need a real backend:
```bash
mvn test -pl hugegraph-server/hugegraph-test -am -P core-test,memory
```

### "What's the difference between hugegraph-server and hugegraph-store?"
- **hugegraph-server**: The graph engine (REST API, Gremlin, schema, traversal)
- **hugegraph-store**: Distributed storage backend (RocksDB + Raft, for production)
- Server can use Store as its backend via `hugegraph-hstore`

### "Where do I find the PD Raft configuration?"
`hugegraph-pd/hg-pd-dist/src/assembly/static/conf/application.yml`
Key settings: `raft.address`, `raft.peers-list`, `grpc.port`

### "How do I enable authentication?"
Authentication is disabled by default. Enable via:
```bash
bin/enable-auth.sh
```
Implementation: `hugegraph-server/hugegraph-api/src/main/java/org/apache/hugegraph/api/auth/`

### "License header check fails"
All Java files must have the Apache License header. Run:
```bash
mvn apache-rat:check
```
Copy the header from any existing file.

### "Where is the Memory System?"
The Memory System lives in the **hugegraph-ai** repository:
`hugegraph-ai/hugegraph-llm/src/hugegraph_llm/`
See: `hugegraph-ai/hugegraph-llm/docs/memory-system-design.md`
