#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Cross-repository query demo for HugeGraph Memory System.
#
# Demonstrates how to use the /memory/query API to perform RAG queries
# across multiple registered repositories.
#
# Usage:
#   ./scripts/memory-cross-repo-query.sh [MEMORY_API_URL]
#
# Examples:
#   ./scripts/memory-cross-repo-query.sh                          # uses default localhost:8000
#   ./scripts/memory-cross-repo-query.sh http://10.0.0.5:8000    # custom endpoint

set -euo pipefail

MEMORY_API="${1:-http://localhost:8000}"

echo "=== HugeGraph Cross-Repo Memory Query Demo ==="
echo "API endpoint : ${MEMORY_API}"
echo ""

# Check if the memory service is reachable
if ! curl -sf "${MEMORY_API}/memory/repos" > /dev/null 2>&1; then
    echo "[ERROR] Cannot reach HugeGraph-AI Memory Service at ${MEMORY_API}"
    exit 1
fi

# Helper function for queries
query_memory() {
    local label="$1"
    local query_text="$2"
    shift 2
    local repo_tags_json="$1"

    echo "--- Query: ${label} ---"
    echo "Q: ${query_text}"
    echo "Repos: ${repo_tags_json}"
    echo ""

    RESPONSE=$(curl -sf -X POST "${MEMORY_API}/memory/query" \
        -H "Content-Type: application/json" \
        -d "{
            \"query\": \"${query_text}\",
            \"repo_tags\": ${repo_tags_json}
        }" 2>&1) || true

    if [ -n "${RESPONSE}" ]; then
        echo "${RESPONSE}" | python3 -m json.tool 2>/dev/null || echo "${RESPONSE}"
    else
        echo "(no response)"
    fi
    echo ""
}

# ---- Demo Queries ----

echo "=========================================="
echo " Demo 1: Single-repo query (core engine)"
echo "=========================================="
query_memory \
    "Backend storage architecture" \
    "How does the BackendStore interface work? What backends are supported?" \
    '[\"incubator-hugegraph\"]'

echo "=========================================="
echo " Demo 2: Cross-repo query (gRPC usage)"
echo "=========================================="
query_memory \
    "gRPC across ecosystem" \
    "How is gRPC used for communication between PD, Store, and Server?" \
    '[\"incubator-hugegraph\", \"hugegraph-toolchain\", \"hugegraph-ai\"]'

echo "=========================================="
echo " Demo 3: Cross-repo query (auth system)"
echo "=========================================="
query_memory \
    "Authentication system" \
    "How does the multi-level authentication system work?" \
    '[\"incubator-hugegraph\", \"hugegraph-toolchain\"]'

echo "=========================================="
echo " Demo 4: AI-specific query"
echo "=========================================="
query_memory \
    "Memory System design" \
    "How does the repo-level Memory System store and query knowledge graphs?" \
    '[\"hugegraph-ai\", \"incubator-hugegraph\"]'

echo "=========================================="
echo " Custom Query"
echo "=========================================="
echo "To run your own query:"
echo ""
echo "  curl -X POST ${MEMORY_API}/memory/query \\"
echo "    -H 'Content-Type: application/json' \\"
echo "    -d '{"
echo '      "query": "Your question here",'
echo '      "repo_tags": ["incubator-hugegraph", "hugegraph-ai"]'
echo "    }'"
echo ""
