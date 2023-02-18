package io.nosqlbench.activitytype.ycql.core;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CqlActivityTest {

    @Test
    public void testCanonicalize() {
        String cb = CqlActivity.canonicalizeBindings("A ?b C");
        assertThat(cb).isEqualTo("A {b} C");
    }
}
