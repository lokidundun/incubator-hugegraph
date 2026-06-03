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

# Register incubator-hugegraph into HugeGraph-AI Memory System.
#
# Prerequisites:
#   - HugeGraph-AI Memory Service running (default: http://localhost:8000)
#   - curl installed
#
# Usage:
#   ./scripts/memory-register.sh [MEMORY_API_URL] [REPO_TAG]

set -euo pipefail

MEMORY_API="${1:-http://localhost:8000}"
REPO_TAG="${2:-incubator-hugegraph}"
REPO_PATH="$(cd "$(dirname "$0")/.." && pwd)"

echo "=== HugeGraph Memory Registration ==="
echo "API endpoint : ${MEMORY_API}"
echo "Repo tag     : ${REPO_TAG}"
echo "Repo path    : ${REPO_PATH}"
echo ""

# Check if the memory service is reachable
if ! curl -sf "${MEMORY_API}/memory/repos" > /dev/null 2>&1; then
    echo "[ERROR] Cannot reach HugeGraph-AI Memory Service at ${MEMORY_API}"
    echo "        Make sure the service is running: cd hugegraph-ai && python -m hugegraph_llm"
    exit 1
fi

# Register the repository
echo "Registering repository..."
RESPONSE=$(curl -sf -X POST "${MEMORY_API}/memory/register" \
    -H "Content-Type: application/json" \
    -d "{
        \"repo_tag\": \"${REPO_TAG}\",
        \"repo_path\": \"${REPO_PATH}\",
        \"extract_type\": \"property_graph\"
    }")

if [ $? -eq 0 ]; then
    echo "[OK] Repository registered successfully."
    echo ""
    echo "Response:"
    echo "${RESPONSE}" | python3 -m json.tool 2>/dev/null || echo "${RESPONSE}"
else
    echo "[ERROR] Registration failed."
    exit 1
fi

# Suggest installing the git hook
echo ""
echo "=== Next Steps ==="
echo "To enable automatic incremental updates on git commit, install the hook:"
echo "  cp scripts/hooks/post-commit-memory .git/hooks/post-commit"
echo "  chmod +x .git/hooks/post-commit"
