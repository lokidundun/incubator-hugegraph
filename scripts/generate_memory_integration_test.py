#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Generate Memory System integration test into hugegraph-ai repository.

Usage:
    python scripts/generate_memory_integration_test.py <hugegraph-ai-path>
"""

import sys
from pathlib import Path


TEST_CONTENT = '''# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""End-to-end integration test for the Memory System.

Tests the full pipeline:
  1. Register repo
  2. Full extraction (all files)
  3. Modify files
  4. Incremental update (only changed files)
  5. Detect deleted files
  6. Maven/Java skip rules
  7. Confidence label injection

Requires a running HugeGraph Server instance (unless using mocks).

Run:
    pytest tests/integration/test_memory_system_e2e.py -v
"""

import os
import shutil
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from hugegraph_llm.utils.memory_manifest import MemoryManifest
from hugegraph_llm.operators.hugegraph_op.incremental_commit import IncrementalCommit


class TestMemorySystemE2E(unittest.TestCase):
    """End-to-end integration test for the Memory System pipeline."""

    @classmethod
    def setUpClass(cls):
        """Create a temporary repo with sample files."""
        cls.tmp_dir = tempfile.mkdtemp(prefix="memory_test_")
        cls.repo_path = os.path.join(cls.tmp_dir, "test-repo")
        os.makedirs(cls.repo_path)

        # Create sample Python files
        cls._write_file("module_a.py", (
            "class Foo:\\n"
            "    \\"\\"\\"A sample class.\\"\\"\\"\\n"
            "    def __init__(self, name):\\n"
            "        self.name = name\\n"
            "    def greet(self):\\n"
            "        return f\\"Hello, {self.name}\\"\\n"
        ))
        cls._write_file("module_b.py", (
            "from module_a import Foo\\n"
            "def create_foo(name):\\n"
            "    return Foo(name)\\n"
        ))
        cls._write_file("README.md", "# Test Repo\\n\\nSample repository for memory tests.\\n")

        # Manifest in a temp directory
        cls.manifest_dir = tempfile.mkdtemp(prefix="memory_manifest_")
        cls.manifest_path = Path(cls.manifest_dir) / "test-manifest.json"

    @classmethod
    def tearDownClass(cls):
        """Clean up temp directories."""
        shutil.rmtree(cls.tmp_dir, ignore_errors=True)
        shutil.rmtree(cls.manifest_dir, ignore_errors=True)

    @classmethod
    def _write_file(cls, name, content):
        path = os.path.join(cls.repo_path, name)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        return path

    def setUp(self):
        """Fresh manifest for each test."""
        self.manifest = MemoryManifest(self.manifest_path)

    # -- Step 1: Register --------------------------------------------------

    def test_01_register_repo(self):
        """Register a test repository."""
        result = self.manifest.register_repo("test-repo", self.repo_path)
        self.assertEqual(result["repo_tag"], "test-repo")

        repos = self.manifest.list_repos()
        self.assertIn("test-repo", repos)

    # -- Step 2: Full scan -------------------------------------------------

    def test_02_scan_and_detect_all_new(self):
        """First scan should report all files as changed."""
        self.manifest.register_repo("test-repo", self.repo_path)

        file_list = MemoryManifest.scan_repo_files(self.repo_path)
        self.assertGreater(len(file_list), 0)

        changes = self.manifest.detect_changes("test-repo", file_list)
        self.assertEqual(len(changes["changed"]), len(file_list))
        self.assertEqual(len(changes["unchanged"]), 0)
        self.assertEqual(len(changes["deleted"]), 0)

    # -- Step 3: No changes after processing -------------------------------

    def test_03_no_changes_after_processing(self):
        """After processing all files, a second scan should find no changes."""
        self.manifest.register_repo("test-repo", self.repo_path)
        file_list = MemoryManifest.scan_repo_files(self.repo_path)
        changes = self.manifest.detect_changes("test-repo", file_list)

        for fpath in changes["changed"]:
            self.manifest.update_file("test-repo", fpath, "fake-hash", 0.0, "code")
        self.manifest.save()

        changes2 = self.manifest.detect_changes("test-repo", file_list)
        self.assertEqual(len(changes2["changed"]), 0)
        self.assertEqual(len(changes2["unchanged"]), len(file_list))

    # -- Step 4: Incremental change detection ------------------------------

    def test_04_incremental_change_detection(self):
        """Modifying one file should produce exactly one changed file."""
        self.manifest.register_repo("test-repo", self.repo_path)
        file_list = MemoryManifest.scan_repo_files(self.repo_path)

        changes = self.manifest.detect_changes("test-repo", file_list)
        for fpath in changes["changed"]:
            self.manifest.update_file("test-repo", fpath, "fake-hash", 0.0, "code")
        self.manifest.save()

        # Modify one file
        time.sleep(0.1)
        target = os.path.join(self.repo_path, "module_a.py")
        with open(target, "w", encoding="utf-8") as f:
            f.write("# Modified content\\nclass Bar:\\n    pass\\n")

        file_list2 = MemoryManifest.scan_repo_files(self.repo_path)
        changes2 = self.manifest.detect_changes("test-repo", file_list2)
        self.assertEqual(len(changes2["changed"]), 1)
        self.assertTrue(changes2["changed"][0].endswith("module_a.py"))

    # -- Step 5: Detect deleted files --------------------------------------

    def test_05_deleted_file_detection(self):
        """Deleting a file should show it in the deleted list."""
        self.manifest.register_repo("test-repo", self.repo_path)
        file_list = MemoryManifest.scan_repo_files(self.repo_path)

        changes = self.manifest.detect_changes("test-repo", file_list)
        for fpath in changes["changed"]:
            self.manifest.update_file("test-repo", fpath, "fake-hash", 0.0, "code")
        self.manifest.save()

        target = os.path.join(self.repo_path, "module_b.py")
        os.remove(target)

        file_list2 = MemoryManifest.scan_repo_files(self.repo_path)
        changes2 = self.manifest.detect_changes("test-repo", file_list2)
        self.assertEqual(len(changes2["deleted"]), 1)
        self.assertTrue(changes2["deleted"][0].endswith("module_b.py"))

    # -- Step 6: Maven/Java skip rules ------------------------------------

    def test_06_maven_skip_rules(self):
        """scan_repo_files should skip target/, build/, and .flattened-pom.xml."""
        maven_dir = os.path.join(self.repo_path, "target")
        os.makedirs(maven_dir, exist_ok=True)
        with open(os.path.join(maven_dir, "Compiled.java"), "w") as f:
            f.write("// compiled output")

        with open(os.path.join(self.repo_path, ".flattened-pom.xml"), "w") as f:
            f.write("<project/>")

        file_list = MemoryManifest.scan_repo_files(self.repo_path)

        self.assertFalse(any("target" in p for p in file_list))
        self.assertFalse(any(".flattened-pom.xml" in p for p in file_list))

    # -- Step 7: Confidence labels in IncrementalCommit --------------------

    @patch("hugegraph_llm.operators.hugegraph_op.incremental_commit.IncrementalCommit.init_schema_if_need")
    def test_07_confidence_labels_injected(self, mock_init_schema):
        """Vertices and edges should get confidence property injected."""
        mock_client = MagicMock()
        mock_client.schema.return_value = MagicMock()
        mock_client.gremlin.return_value.exec.return_value = 0
        mock_graph = MagicMock()
        mock_client.graph.return_value = mock_graph
        mock_graph.addVertex.return_value = MagicMock(id="v1")
        mock_graph.addEdge.return_value = MagicMock()

        commit = IncrementalCommit(client=mock_client)
        schema = {
            "propertykeys": [
                {"name": "name", "data_type": "TEXT", "cardinality": "SINGLE"},
                {"name": "repo_tag", "data_type": "TEXT", "cardinality": "SINGLE"},
                {"name": "confidence", "data_type": "TEXT", "cardinality": "SINGLE"},
            ],
            "vertexlabels": [
                {"name": "cls", "properties": ["name", "repo_tag", "confidence"],
                 "primary_keys": ["name"], "nullable_keys": ["repo_tag", "confidence"]},
            ],
            "edgelabels": [
                {"name": "depends_on", "properties": ["repo_tag", "confidence"],
                 "source_label": "cls", "target_label": "cls"},
            ],
        }

        data = {
            "schema": schema,
            "vertices": [
                {"label": "cls", "properties": {"name": "Foo"},
                 "confidence": "INFERRED", "source_file": "/a.py"},
            ],
            "edges": [
                {"label": "depends_on", "outV": "v1", "inV": "v2",
                 "properties": {}, "confidence": "EXTRACTED", "source_file": "/a.py"},
            ],
            "repo_tag": "test-repo",
            "changed_files": ["/a.py"],
            "deleted_files": [],
        }

        result = commit.run(data)

        # Check vertex has confidence injected
        vertex_calls = mock_graph.addVertex.call_args_list
        cls_call = next(c for c in vertex_calls if c[0][0] == "cls")
        self.assertEqual(cls_call[0][1]["confidence"], "INFERRED")
        self.assertEqual(cls_call[0][1]["repo_tag"], "test-repo")

        # Check edge has confidence injected
        edge_calls = mock_graph.addEdge.call_args_list
        self.assertEqual(edge_calls[0][0][3]["confidence"], "EXTRACTED")

        self.assertEqual(result["vertices_committed"], 1)
        self.assertEqual(result["edges_committed"], 1)


if __name__ == "__main__":
    unittest.main()
'''


def main():
    if len(sys.argv) < 2:
        print("Usage: python generate_memory_integration_test.py <hugegraph-ai-path>")
        sys.exit(1)

    ai_root = Path(sys.argv[1]).resolve()
    test_dir = ai_root / "hugegraph-llm" / "src" / "tests" / "integration"
    test_dir.mkdir(parents=True, exist_ok=True)

    test_path = test_dir / "test_memory_system_e2e.py"
    test_path.write_text(TEST_CONTENT, encoding="utf-8")
    print(f"[OK] Created: {test_path}")
    print(f"\nRun with: pytest {test_path} -v")


if __name__ == "__main__":
    main()
