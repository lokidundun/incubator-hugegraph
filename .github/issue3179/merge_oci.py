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

"""Join tested native OCI archives locally, without publishing architecture tags."""
import hashlib
import io
import json
import pathlib
import tarfile
import sys

output = pathlib.Path(sys.argv[1])
output.mkdir(parents=True, exist_ok=True)
tag = sys.argv[2]
for component in ('pd', 'store', 'server', 'standalone'):
    archives = [tarfile.open(f'images/{arch}/{component}.tar') for arch in ('amd64', 'arm64')]
    blobs = {}
    manifests = []
    for arch, archive in zip(('amd64', 'arm64'), archives):
        members = {m.name.removeprefix('./'): m for m in archive.getmembers() if m.isfile()}
        for name, member in members.items():
            if name.startswith('blobs/sha256/'):
                blobs.setdefault(name, (archive, member))

        def walk(descriptor):
            content = json.load(archive.extractfile(members['blobs/' + descriptor['digest'].replace(':', '/')]))
            if 'manifests' in content:
                for child in content['manifests']:
                    walk(child)
            elif 'config' in content:
                config_path = 'blobs/' + content['config']['digest'].replace(':', '/')
                config = json.load(archive.extractfile(members[config_path]))
                if config.get('os') == 'linux' and config.get('architecture') == arch:
                    leaf = dict(descriptor)
                    leaf['platform'] = {'os': 'linux', 'architecture': arch}
                    leaf.pop('annotations', None)
                    manifests.append(leaf)

        for descriptor in json.load(archive.extractfile(members['index.json']))['manifests']:
            walk(descriptor)
    assert sorted(m['platform']['architecture'] for m in manifests) == ['amd64', 'arm64']
    media = 'application/vnd.oci.image.index.v1+json'
    merged = json.dumps({'schemaVersion': 2, 'mediaType': media, 'manifests': manifests}).encode()
    digest = hashlib.sha256(merged).hexdigest()
    image = f'ghcr.io/lokidundun/issue3179-{component}:{tag}'
    index = {'schemaVersion': 2, 'mediaType': media, 'manifests': [
        {'mediaType': media, 'digest': 'sha256:' + digest, 'size': len(merged),
         'annotations': {'io.containerd.image.name': image,
                         'org.opencontainers.image.ref.name': image}}]}
    with tarfile.open(output / f'{component}.tar', 'w') as target:
        for name, data in [('oci-layout', b'{"imageLayoutVersion":"1.0.0"}'),
                           ('index.json', json.dumps(index).encode()), ('blobs/sha256/' + digest, merged)]:
            info = tarfile.TarInfo(name)
            info.size = len(data)
            target.addfile(info, io.BytesIO(data))
        for name, (archive, member) in blobs.items():
            info = tarfile.TarInfo(name)
            info.size = member.size
            target.addfile(info, archive.extractfile(member))
    for archive in archives:
        archive.close()
    print(f'Merged {image}: amd64, arm64')
