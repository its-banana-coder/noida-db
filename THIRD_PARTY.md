# Third-party code

noida-db is MIT licensed. This file lists code that was ported or copied from
other projects, with its licence. Crates pulled in through `Cargo.toml` carry
their own licences (`cargo tree` lists them) and are not repeated here.

Add a row whenever you port code, and keep the upstream copyright notice.

| What | From | Licence | Where |
|---|---|---|---|
| Command implementations, error texts, encodings and algorithms: strings, keys, hashes, lists, sets, sorted sets, streams, geo, bitmaps, HyperLogLog (including MurmurHash64A), SORT, MONITOR, scripting glue, CONFIG, ACL/SLOWLOG/LATENCY/MEMORY replies, command metadata (`src/redis/meta.rs` is generated from `commands.def`) | Redis 7.2, `src/*.c` | BSD-3-Clause | `src/redis/` |
| `cmsgpack` Lua library (`pack`, `unpack`, `unpack_one`, `unpack_limit`) | lua-cmsgpack 0.4.0 as shipped in Redis 7.2, `deps/lua/src/lua_cmsgpack.c` | BSD-2-Clause, Copyright (C) 2012 Salvatore Sanfilippo | `src/redis/cmsgpack.rs` |
| BM25 relevance scoring and the lossy per-document field-length norm encoding (`SmallFloat.intToByte4`/`byte4ToInt`, `BM25Similarity`'s score formula) | Apache Lucene 9.x, `lucene/core/src/java/org/apache/lucene/{util/SmallFloat.java,search/similarities/BM25Similarity.java}` | Apache License 2.0 | `src/elasticsearch/scoring.rs` |

## Redis (BSD-3-Clause)

Copyright (c) 2006-2020, Salvatore Sanfilippo
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

  * Redistributions of source code must retain the above copyright notice,
    this list of conditions and the following disclaimer.
  * Redistributions in binary form must reproduce the above copyright notice,
    this list of conditions and the following disclaimer in the documentation
    and/or other materials provided with the distribution.
  * Neither the name of Redis nor the names of its contributors may be used
    to endorse or promote products derived from this software without
    specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

## Apache Lucene (Apache License 2.0)

Copyright 2001-2024 The Apache Software Foundation

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy
of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations
under the License.

## lua-cmsgpack (BSD-2-Clause)

Copyright (C) 2012 Salvatore Sanfilippo <antirez@gmail.com>

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
