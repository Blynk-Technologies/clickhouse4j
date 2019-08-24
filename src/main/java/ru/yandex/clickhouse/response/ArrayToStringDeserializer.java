package ru.yandex.clickhouse.response;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.type.TypeFactory;
import ru.yandex.clickhouse.Jackson;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class ArrayToStringDeserializer extends JsonDeserializer<List<String>> {

    @Override
    public List<String> deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
        JsonDeserializer<Object> deserializer = getJsonDeserializer(ctxt);

        final Object deserialized = deserializer.deserialize(jp, ctxt);
        if (!(deserialized instanceof List)){
            throw new IllegalStateException();
        }

        final List deserializedList = (List) deserialized;
        List<String> result = new ArrayList<>();
        for (Object x : deserializedList) {
            String v = null;
            if (x instanceof List) {
                try {
                    v = Jackson.getObjectMapper().writeValueAsString(x);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            } else if (x != null) {
                v = x.toString();
            }
            result.add(v);
        }
        return result;
    }

    private JsonDeserializer<Object> getJsonDeserializer(DeserializationContext ctxt) {
        try {
            TypeReference<List<Object>> typeRef = new TypeReference<>() { };
            JavaType type = TypeFactory.defaultInstance().constructType(typeRef);

            return ctxt.findContextualValueDeserializer(type, null);
        } catch (JsonMappingException e) {
            throw new RuntimeException(e);
        }
    }
}
