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

# Register all HugeGraph ecosystem repositories into the Memory System.
#
# Usage:
#   ./scripts/memory-federation-register.sh [MEMORY_API_URL] [BASE_DIR]
#
# Arguments:
#   MEMORY_API_URL  - HugeGraph-AI Memory Service URL (default: http://localhost:8000)
#   BASE_DIR        - Parent directory containing all repos (default: ..)
#
# Expected directory layout (repos as siblings):
#   BASE_DIR/
#     incubator-hugegraph/
#     hugegraph-toolchain/
#     hugegraph-ai/
#     hugegraph-computer/
#     hugegraph-doc/

set -euo pipefail

MEMORY_API="${1:-http://localhost:8000}"
BASE_DIR="${2:-$(cd "$(dirname "$0")/../.." && pwd)}"

echo "=== HugeGraph Memory Federation Registration ==="
echo "API endpoint : ${MEMORY_API}"
echo "Base dir     : ${BASE_DIR}"
echo ""

# Check if the memory service is reachable
if ! curl -sf "${MEMORY_API}/memory/repos" > /dev/null 2>&1; then
    echo "[ERROR] Cannot reach HugeGraph-AI Memory Service at ${MEMORY_API}"
    echo "        Start the service: cd hugegraph-ai && python -m hugegraph_llm"
    exit 1
fi

# Repository definitions: tag|subdir|description
REPOS=(
    "incubator-hugegraph|incubator-hugegraph|Core graph database (server, pd, store)"
    "hugegraph-toolchain|hugegraph-toolchain|Loader, Hubble, Client, Tools"
    "hugegraph-ai|hugegraph-ai|LLM, KG, Graph RAG, Memory System"
    "hugegraph-computer|hugegraph-computer|Distributed graph computing (OLAP)"
    "hugegraph-doc|hugegraph-doc|Documentation and website"
)

REGISTERED=0
SKIPPED=0
FAILED=0

for entry in "${REPOS[@]}"; do
    IFS='|' read -r tag dir desc <<< "${entry}"
    repo_path="${BASE_DIR}/${dir}"

    if [ ! -d "${repo_path}" ]; then
        echo "[SKIP] ${tag}: directory not found at ${repo_path}"
        SKIPPED=$((SKIPPED + 1))
        continue
    fi

    echo -n "Registering ${tag} (${desc})... "

    RESPONSE=$(curl -sf -X POST "${MEMORY_API}/memory/register" \
        -H "Content-Type: application/json" \
        -d "{
            \"repo_tag\": \"${tag}\",
            \"repo_path\": \"${repo_path}\",
            \"extract_type\": \"property_graph\"
        }" 2>&1) || true

    if [ $? -eq 0 ] && [ -n "${RESPONSE}" ]; then
        echo "[OK]"
        REGISTERED=$((REGISTERED + 1))
    else
        echo "[FAIL]"
        FAILED=$((FAILED + 1))
    fi
done

echo ""
echo "=== Summary ==="
echo "Registered : ${REGISTERED}"
echo "Skipped    : ${SKIPPED}"
echo "Failed     : ${FAILED}"
echo ""

# Show all registered repos
echo "=== Registered Repos ==="
curl -sf "${MEMORY_API}/memory/repos" | python3 -m json.tool 2>/dev/null || echo "(unable to fetch repo list)"

echo ""
echo "=== Next Steps ==="
echo "Run initial full extraction for each repo:"
echo "  curl -X POST ${MEMORY_API}/memory/update \\"
echo "    -H 'Content-Type: application/json' \\"
echo "    -d '{\"repo_tag\": \"<tag>\", \"force\": true}'"
echo ""
echo "Then try a cross-repo query:"
echo "  ./scripts/memory-cross-repo-query.sh '${MEMORY_API}'"
