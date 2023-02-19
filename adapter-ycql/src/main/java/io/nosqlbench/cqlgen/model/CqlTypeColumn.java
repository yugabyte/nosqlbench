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

package io.nosqlbench.cqlgen.model;

import java.util.LinkedHashMap;
import java.util.Map;

public class CqlTypeColumn extends CqlColumnBase {

    CqlType type;

    public CqlTypeColumn(String colname, String typedef, CqlType usertype) {
        super(colname, typedef);
        this.setType(usertype);
    }

    @Override
    protected String getParentFullName() {
        return type.getFullName();
    }

    public CqlType getType() {
        return type;
    }

    public void setType(CqlType type) {
        this.type = type;
    }

    @Override
    public Map<String, String> getLabels() {
        Map<String,String> map = new LinkedHashMap<>(super.getLabels());
        map.put("name",type.getName());
        return map;
    }
}
