package cc.blynk.clickhouse.integration.json;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public final class DateTimeIntEntry {

    public final String created;

    public final int value;

    @JsonCreator
    public DateTimeIntEntry(@JsonProperty("created") String created,
                            @JsonProperty("value") int value) {
        this.created = created;
        this.value = value;
    }
}
