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

"""Apply Memory System V1 -> V2 patches to hugegraph-ai repository.

This script applies the following changes to hugegraph-ai/hugegraph-llm:

1. memory_manifest.py: Add DEFAULT_SKIP_DIRS / DEFAULT_SKIP_FILES for Java/Maven projects
2. incremental_commit.py: Add confidence label injection (EXTRACTED/INFERRED/AMBIGUOUS)

Usage:
    python scripts/apply-memory-v2-patches.py <hugegraph-ai-path>

Example:
    python scripts/apply-memory-v2-patches.py ../hugegraph-ai
"""

import os
import re
import sys
from pathlib import Path


def patch_memory_manifest(llm_root: Path) -> bool:
    """Patch memory_manifest.py with Java/Maven skip rules."""
    target = llm_root / "src" / "hugegraph_llm" / "utils" / "memory_manifest.py"
    if not target.exists():
        print(f"[ERROR] File not found: {target}")
        return False

    content = target.read_text(encoding="utf-8")

    # 1. Add DEFAULT_SKIP_DIRS and DEFAULT_SKIP_FILES after DEFAULT_EXTENSIONS
    old_extensions_block = '''# Default file extensions to process (code + docs)
DEFAULT_EXTENSIONS = {
    ".py", ".js", ".ts", ".tsx", ".jsx", ".java", ".go", ".rs", ".c", ".cpp",
    ".h", ".hpp", ".rb", ".php", ".cs", ".swift", ".kt", ".scala", ".lua",
    ".md", ".txt", ".rst", ".json", ".yaml", ".yml", ".toml", ".xml",
}'''

    new_extensions_block = '''# Default file extensions to process (code + docs)
DEFAULT_EXTENSIONS = {
    ".py", ".js", ".ts", ".tsx", ".jsx", ".java", ".go", ".rs", ".c", ".cpp",
    ".h", ".hpp", ".rb", ".php", ".cs", ".swift", ".kt", ".scala", ".lua",
    ".md", ".txt", ".rst", ".json", ".yaml", ".yml", ".toml", ".xml",
}

# Directories to skip during repo scanning (build artifacts, caches, etc.)
DEFAULT_SKIP_DIRS = {
    ".git", ".hg", ".svn",                          # VCS
    "node_modules", "__pycache__", ".mypy_cache",     # Language caches
    "venv", ".venv", ".tox",                         # Python environments
    "target", "build", "out",                         # Java/Maven/Gradle build output
    ".idea", ".vscode", ".settings", ".eclipse",   # IDE config
    ".gradle", ".mvn",                               # Build tool caches
    "dist", ".eggs", "*.egg-info",                    # Python dist artifacts
}

# File patterns to skip (build-generated files)
DEFAULT_SKIP_FILES = {
    ".flattened-pom.xml",                             # Maven flatten plugin output
    "pom.xml.tag", "pom.xml.releaseBackup",
    "pom.xml.next", "pom.xml.versionsBackup",
    "release.properties",                             # Maven release plugin
    "dependency-reduced-pom.xml",                     # Maven shade plugin
}'''

    if old_extensions_block not in content:
        print("[WARN] DEFAULT_EXTENSIONS block not found as expected, skipping manifest patch")
        return False

    content = content.replace(old_extensions_block, new_extensions_block, 1)

    # 2. Replace scan_repo_files method
    old_scan = '''    @staticmethod
    def scan_repo_files(repo_path: str, extensions: Optional[Set[str]] = None) -> List[str]:
        """Walk a repository directory and return absolute file paths."""
        if extensions is None:
            extensions = DEFAULT_EXTENSIONS
        root = Path(repo_path)
        if not root.is_dir():
            return []
        result = []
        for p in root.rglob("*"):
            if p.is_file() and p.suffix.lower() in extensions:
                # Skip hidden directories and common non-source dirs
                parts = p.relative_to(root).parts
                if any(part.startswith(".") or part in ("node_modules", "__pycache__", ".git", "venv", ".venv")
                       for part in parts):
                    continue
                result.append(str(p.resolve()))
        return result'''

    new_scan = '''    @staticmethod
    def scan_repo_files(
        repo_path: str,
        extensions: Optional[Set[str]] = None,
        skip_dirs: Optional[Set[str]] = None,
        skip_files: Optional[Set[str]] = None,
    ) -> List[str]:
        """Walk a repository directory and return absolute file paths.

        Args:
            repo_path: Root directory of the repository.
            extensions: File extensions to include (default: DEFAULT_EXTENSIONS).
            skip_dirs: Directory names to skip (default: DEFAULT_SKIP_DIRS).
            skip_files: File names to skip (default: DEFAULT_SKIP_FILES).
        """
        if extensions is None:
            extensions = DEFAULT_EXTENSIONS
        if skip_dirs is None:
            skip_dirs = DEFAULT_SKIP_DIRS
        if skip_files is None:
            skip_files = DEFAULT_SKIP_FILES
        root = Path(repo_path)
        if not root.is_dir():
            return []
        result = []
        for p in root.rglob("*"):
            if not p.is_file():
                continue
            # Skip files with excluded extensions
            if extensions and p.suffix.lower() not in extensions:
                continue
            # Skip files in excluded directories
            parts = p.relative_to(root).parts
            if any(part in skip_dirs or part.startswith(".") for part in parts):
                continue
            # Skip excluded file names
            if p.name in skip_files:
                continue
            result.append(str(p.resolve()))
        return result'''

    if old_scan not in content:
        print("[WARN] scan_repo_files block not found as expected, skipping scan patch")
        return False

    content = content.replace(old_scan, new_scan, 1)

    target.write_text(content, encoding="utf-8")
    print(f"[OK] Patched: {target}")
    return True


def patch_incremental_commit(llm_root: Path) -> bool:
    """Patch incremental_commit.py with confidence label injection."""
    target = llm_root / "src" / "hugegraph_llm" / "operators" / "hugegraph_op" / "incremental_commit.py"
    if not target.exists():
        print(f"[ERROR] File not found: {target}")
        return False

    content = target.read_text(encoding="utf-8")

    # Add confidence label injection into vertex commit loop
    old_vertex_block = '''        committed_v = 0
        for vertex in vertices:
            props = dict(vertex.get("properties", {}))
            props["repo_tag"] = repo_tag
            if vertex.get("source_file"):
                props["source_file"] = vertex["source_file"]
            label = vertex.get("label", "vertex")'''

    new_vertex_block = '''        committed_v = 0
        for vertex in vertices:
            props = dict(vertex.get("properties", {}))
            props["repo_tag"] = repo_tag
            if vertex.get("source_file"):
                props["source_file"] = vertex["source_file"]
            # Inject confidence label (inspired by Graphify's EXTRACTED/INFERRED/AMBIGUOUS)
            confidence = vertex.get("confidence", "EXTRACTED")
            if confidence in ("EXTRACTED", "INFERRED", "AMBIGUOUS"):
                props["confidence"] = confidence
            label = vertex.get("label", "vertex")'''

    if old_vertex_block not in content:
        print("[WARN] Vertex commit block not found as expected, skipping vertex patch")
        return False

    content = content.replace(old_vertex_block, new_vertex_block, 1)

    # Add confidence label injection into edge commit loop
    old_edge_block = '''        committed_e = 0
        for edge in edges:
            props = dict(edge.get("properties", {}))
            props["repo_tag"] = repo_tag
            if edge.get("source_file"):
                props["source_file"] = edge["source_file"]'''

    new_edge_block = '''        committed_e = 0
        for edge in edges:
            props = dict(edge.get("properties", {}))
            props["repo_tag"] = repo_tag
            if edge.get("source_file"):
                props["source_file"] = edge["source_file"]
            # Inject confidence label
            confidence = edge.get("confidence", "EXTRACTED")
            if confidence in ("EXTRACTED", "INFERRED", "AMBIGUOUS"):
                props["confidence"] = confidence'''

    if old_edge_block not in content:
        print("[WARN] Edge commit block not found as expected, skipping edge patch")
        return False

    content = content.replace(old_edge_block, new_edge_block, 1)

    target.write_text(content, encoding="utf-8")
    print(f"[OK] Patched: {target}")
    return True


def main():
    if len(sys.argv) < 2:
        print("Usage: python apply-memory-v2-patches.py <hugegraph-ai-path>")
        print("Example: python apply-memory-v2-patches.py ../hugegraph-ai")
        sys.exit(1)

    ai_root = Path(sys.argv[1]).resolve()
    llm_root = ai_root / "hugegraph-llm"

    if not llm_root.is_dir():
        print(f"[ERROR] hugegraph-llm directory not found at: {llm_root}")
        sys.exit(1)

    print(f"=== Applying Memory V2 Patches ===")
    print(f"Target: {llm_root}")
    print()

    results = []
    results.append(("memory_manifest.py (Java/Maven skip)", patch_memory_manifest(llm_root)))
    results.append(("incremental_commit.py (confidence labels)", patch_incremental_commit(llm_root)))

    print()
    print("=== Summary ===")
    for name, ok in results:
        status = "OK" if ok else "FAILED"
        print(f"  [{status}] {name}")

    if all(ok for _, ok in results):
        print("\nAll patches applied successfully!")
        sys.exit(0)
    else:
        print("\nSome patches failed. Check the output above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
