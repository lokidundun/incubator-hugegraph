# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

"""Compare unpacked distribution contents, including nested JAR entries."""
import hashlib
import io
import json
import pathlib
import stat
import sys
import zipfile


def digest(data, name, depth=0):
    if depth < 5 and name.endswith(('.jar', '.zip')) and zipfile.is_zipfile(io.BytesIO(data)):
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            entries = [(entry.filename, digest(archive.read(entry), entry.filename, depth + 1))
                       for entry in archive.infolist() if not entry.is_dir()]
        return hashlib.sha256(json.dumps(sorted(entries)).encode()).hexdigest()
    if name.endswith('pom.properties'):
        # Properties comments contain packaging dates, not application content.
        data = b'\n'.join(sorted(line for line in data.splitlines()
                                  if line.strip() and not line.startswith((b'#', b'!'))))
    if name.endswith('git.properties'):
        data = b'\n'.join(sorted(line for line in data.splitlines()
                                  if not line.startswith((b'#', b'git.build.time=')) and line.strip()))
    return hashlib.sha256(data).hexdigest()


def inventory(root):
    result = {}
    for path in sorted(root.rglob('*')):
        key = path.relative_to(root).as_posix()
        if path.is_symlink():
            result[key] = {'link': str(path.readlink())}
        elif path.is_file():
            result[key] = {'mode': stat.S_IMODE(path.stat().st_mode),
                           'content': digest(path.read_bytes(), key)}
    return result


if __name__ == '__main__':
    if sys.argv[1] == 'record':
        pathlib.Path(sys.argv[3]).write_text(json.dumps(inventory(pathlib.Path(sys.argv[2])), indent=2))
    else:
        before, after = [json.loads(pathlib.Path(p).read_text()) for p in sys.argv[2:4]]
        result = {'missing': sorted(before.keys() - after.keys()),
                  'added': sorted(after.keys() - before.keys()),
                  'changed': [k for k in sorted(before.keys() & after.keys()) if before[k] != after[k]],
                  'full_files': len(before), 'scoped_files': len(after),
                  'normalization': ['ZIP entry timestamps/compression/order', 'pom.properties comments',
                                    'git.properties comments and git.build.time']}
        pathlib.Path(sys.argv[4]).write_text(json.dumps(result, indent=2))
        print(json.dumps(result, indent=2))
        sys.exit(bool(result['missing'] or result['added'] or result['changed']))
