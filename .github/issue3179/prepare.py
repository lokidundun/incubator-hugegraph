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

"""Fork-only experiment preparation; leaves the proposed Dockerfiles untouched."""
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
mode = sys.argv[2]
paths = {'pd': 'hugegraph-pd/Dockerfile', 'store': 'hugegraph-store/Dockerfile',
         'server-hstore': 'hugegraph-server/Dockerfile-hstore',
         'server-standalone': 'hugegraph-server/Dockerfile'}
if mode == 'distribution':
    source = (root / paths['pd']).read_text()
    source = source[:source.index('FROM eclipse-temurin:')]
    source += '\nFROM scratch AS distributions\n'
    for name in ('pd', 'store', 'server'):
        source += f'COPY --from=build /pkg/hugegraph-{name}/apache-hugegraph-{name}-*/ /{name}/\n'
    (root / 'Distribution.Dockerfile').write_text(source)
elif mode == 'runtime':
    for name, path in paths.items():
        source = (root / path).read_text()
        source = '# syntax=docker/dockerfile:1\n' + source[source.index('FROM eclipse-temurin:'):]
        component = name if name in ('pd', 'store') else 'server'
        source = source.replace(f'COPY --from=build /pkg/hugegraph-{component}/apache-hugegraph-{component}-*/',
                                f'COPY dist/{component}/')
        assert '--from=build' not in source
        (root / path).write_text(source)
    (root / '.dockerignore').write_text((root / '.dockerignore').read_text() + '\n!dist/**\n')
else:
    raise ValueError(mode)
