package cc.blynk.clickhouse.integration.json;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public final class LongIntEntry {

    public final long created;

    public final int value;

    @JsonCreator
    public LongIntEntry(@JsonProperty("created") long created,
                        @JsonProperty("value") int value) {
        this.created = created;
        this.value = value;
    }
}
