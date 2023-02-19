/*
 * Copyright (c) 2022 nosqlbench
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.nosqlbench.cqlgen.transformers;

import io.nosqlbench.cqlgen.api.CGModelTransformer;
import io.nosqlbench.cqlgen.api.CGTransformerConfigurable;
import io.nosqlbench.cqlgen.model.CqlKeyspaceDef;
import io.nosqlbench.cqlgen.model.CqlModel;

import java.util.Map;

public class CGReplicationSettingInjector implements CGModelTransformer, CGTransformerConfigurable {
    private String replicationFields;
    private String name;

    @Override
    public CqlModel apply(CqlModel model) {
        for (CqlKeyspaceDef keyspace : model.getKeyspaceDefs()) {
            keyspace.setReplicationData(this.replicationFields);
        }
        return model;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public void accept(Object cfgObject) {
        if (cfgObject instanceof Map stringMap) {
            if (stringMap.containsKey("replication_fields")) {
                this.replicationFields = stringMap.get("replication_fields").toString();
            }
        } else {
            throw new RuntimeException("replication settings injector requires a map for its config value.");
        }
    }

    @Override
    public String getName() {
        return this.name;
    }
}
