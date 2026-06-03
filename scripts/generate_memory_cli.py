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

"""Generate Memory Health Check CLI module into hugegraph-ai repository."""

import sys
from pathlib import Path

CLI_CONTENT = r'''# Licensed to the Apache Software Foundation (ASF) under one
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

"""Memory Health Check CLI.

Commands:
    doctor  - Check health status of all registered repos
    sync    - Trigger incremental memory update for a repo
    stats   - Show statistics for a specific repo

Usage:
    python -m hugegraph_llm.commands.memory_cli doctor
    python -m hugegraph_llm.commands.memory_cli sync <repo_tag> [--force]
    python -m hugegraph_llm.commands.memory_cli stats <repo_tag>

Inspired by Gbrain's `gbrain doctor --remediation-plan`.
"""

import argparse
import json
import sys
from pathlib import Path

from hugegraph_llm.utils.memory_manifest import MemoryManifest


# Health check thresholds
DRIFT_WARN_THRESHOLD = 0.05  # 5% file drift triggers warning
DRIFT_ERROR_THRESHOLD = 0.20  # 20% file drift triggers error


def cmd_doctor(args: argparse.Namespace) -> int:
    """Check memory health for all registered repos."""
    manifest = MemoryManifest(Path(args.manifest) if args.manifest else None)
    repos = manifest.list_repos()

    if not repos:
        print("No repositories registered.")
        return 0

    print(f"{'='*60}")
    print(f" Memory Health Report")
    print(f"{'='*60}\n")

    all_healthy = True
    for tag, info in repos.items():
        repo_path = info.get("repo_path", "")
        file_count = info.get("file_count", 0)
        node_count = info.get("node_count", 0)
        edge_count = info.get("edge_count", 0)
        updated_at = info.get("updated_at", "never")

        # Check files on disk vs manifest
        disk_files = MemoryManifest.scan_repo_files(repo_path) if Path(repo_path).is_dir() else []
        disk_count = len(disk_files)

        if file_count > 0:
            drift = abs(disk_count - file_count) / max(file_count, 1)
        else:
            drift = 1.0 if disk_count > 0 else 0.0

        # Determine status
        if drift > DRIFT_ERROR_THRESHOLD:
            status = "NEEDS_UPDATE"
            status_icon = "[!!]"
            all_healthy = False
        elif drift > DRIFT_WARN_THRESHOLD:
            status = "MINOR_DRIFT"
            status_icon = "[!]"
        elif node_count == 0 and disk_count > 0:
            status = "NOT_EXTRACTED"
            status_icon = "[!!]"
            all_healthy = False
        else:
            status = "HEALTHY"
            status_icon = "[OK]"

        print(f"  {status_icon} Repo: {tag}")
        print(f"      Path          : {repo_path}")
        print(f"      Files on disk : {disk_count}")
        print(f"      In manifest   : {file_count}")
        print(f"      Drift         : {drift:.1%}")
        print(f"      Vertices      : {node_count}")
        print(f"      Edges         : {edge_count}")
        print(f"      Last update   : {updated_at}")
        print(f"      Status        : {status}")
        print()

    print(f"{'='*60}")
    if all_healthy:
        print("  All repositories are healthy.")
    else:
        print("  Some repositories need attention.")
        print("  Run 'sync <repo_tag>' to update.")
    print(f"{'='*60}")

    return 0 if all_healthy else 1


def cmd_sync(args: argparse.Namespace) -> int:
    """Trigger incremental memory update for a repo."""
    manifest = MemoryManifest(Path(args.manifest) if args.manifest else None)

    try:
        info = manifest.get_repo_info(args.repo_tag)
    except KeyError:
        print(f"[ERROR] Repo '{args.repo_tag}' not found in manifest.")
        print(f"        Register it first: POST /memory/register")
        return 1

    repo_path = info.get("repo_path", "")
    if not Path(repo_path).is_dir():
        print(f"[ERROR] Repo path does not exist: {repo_path}")
        return 1

    # Detect changes
    file_list = MemoryManifest.scan_repo_files(repo_path)
    changes = manifest.detect_changes(args.repo_tag, file_list)

    changed = changes.get("changed", [])
    deleted = changes.get("deleted", [])
    unchanged = changes.get("unchanged", [])

    print(f"Repo: {args.repo_tag}")
    print(f"  Changed   : {len(changed)} files")
    print(f"  Deleted   : {len(deleted)} files")
    print(f"  Unchanged : {len(unchanged)} files")

    if args.force:
        print(f"  Mode      : FORCE (re-process all files)")
        print(f"\n  To trigger full extraction via API:")
        print(f"    curl -X POST http://localhost:8000/memory/update \\")
        print(f'      -H "Content-Type: application/json" \\')
        print(f'      -d \'{{"repo_tag": "{args.repo_tag}", "force": true}}\'')
    elif not changed and not deleted:
        print(f"\n  No changes detected. Memory is up to date.")
    else:
        print(f"\n  To trigger incremental update via API:")
        print(f"    curl -X POST http://localhost:8000/memory/update \\")
        print(f'      -H "Content-Type: application/json" \\')
        print(f'      -d \'{{"repo_tag": "{args.repo_tag}"}}\'')

    return 0


def cmd_stats(args: argparse.Namespace) -> int:
    """Show detailed statistics for a repo."""
    manifest = MemoryManifest(Path(args.manifest) if args.manifest else None)

    try:
        info = manifest.get_repo_info(args.repo_tag)
    except KeyError:
        print(f"[ERROR] Repo '{args.repo_tag}' not found in manifest.")
        return 1

    repo_path = info.get("repo_path", "")
    print(f"=== Repo Statistics: {args.repo_tag} ===\n")
    print(f"  Path             : {repo_path}")
    print(f"  Registered at    : {info.get('registered_at', 'unknown')}")
    print(f"  Last updated     : {info.get('updated_at', 'never')}")
    print(f"  Manifest files   : {info.get('file_count', 0)}")
    print(f"  Vertices         : {info.get('node_count', 0)}")
    print(f"  Edges            : {info.get('edge_count', 0)}")

    if Path(repo_path).is_dir():
        disk_files = MemoryManifest.scan_repo_files(repo_path)
        print(f"  Files on disk    : {len(disk_files)}")

        # Extension breakdown
        ext_counts = {}
        for f in disk_files:
            ext = Path(f).suffix.lower()
            ext_counts[ext] = ext_counts.get(ext, 0) + 1
        if ext_counts:
            print(f"\n  Extension breakdown:")
            for ext, count in sorted(ext_counts.items(), key=lambda x: -x[1]):
                print(f"    {ext or '(none)'}: {count}")

    return 0


def main():
    parser = argparse.ArgumentParser(
        prog="memory_cli",
        description="HugeGraph Memory System Health Check CLI",
    )
    parser.add_argument(
        "--manifest", type=str, default=None,
        help="Path to manifest JSON (default: ~/.hugegraph/memory-manifest.json)",
    )
    sub = parser.add_subparsers(dest="command", help="Sub-command")

    # doctor
    sub.add_parser("doctor", help="Check health of all registered repos")

    # sync
    p_sync = sub.add_parser("sync", help="Trigger memory update for a repo")
    p_sync.add_argument("repo_tag", help="Repository tag")
    p_sync.add_argument("--force", action="store_true", help="Force full re-extraction")

    # stats
    p_stats = sub.add_parser("stats", help="Show statistics for a repo")
    p_stats.add_argument("repo_tag", help="Repository tag")

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        return 0

    handlers = {
        "doctor": cmd_doctor,
        "sync": cmd_sync,
        "stats": cmd_stats,
    }
    return handlers[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
'''

INIT_CONTENT = '''# Licensed to the Apache Software Foundation (ASF) under one
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
'''


def main():
    if len(sys.argv) < 2:
        print("Usage: python generate_memory_cli.py <hugegraph-ai-path>")
        sys.exit(1)

    ai_root = Path(sys.argv[1]).resolve()
    cmd_dir = ai_root / "hugegraph-llm" / "src" / "hugegraph_llm" / "commands"
    cmd_dir.mkdir(parents=True, exist_ok=True)

    # Write __init__.py
    init_path = cmd_dir / "__init__.py"
    init_path.write_text(INIT_CONTENT, encoding="utf-8")
    print(f"[OK] Created: {init_path}")

    # Write memory_cli.py
    cli_path = cmd_dir / "memory_cli.py"
    cli_path.write_text(CLI_CONTENT, encoding="utf-8")
    print(f"[OK] Created: {cli_path}")

    print("\nMemory Health Check CLI installed successfully!")
    print("Usage:")
    print("  python -m hugegraph_llm.commands.memory_cli doctor")
    print("  python -m hugegraph_llm.commands.memory_cli sync <repo_tag>")
    print("  python -m hugegraph_llm.commands.memory_cli stats <repo_tag>")


if __name__ == "__main__":
    main()
