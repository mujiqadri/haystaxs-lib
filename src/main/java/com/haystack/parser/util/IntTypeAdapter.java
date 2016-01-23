package com.haystack.parser.util;

import com.google.gson.JsonSyntaxException;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;

/**
 * Created by qadrim on 16-01-17.
 */

public class IntTypeAdapter extends TypeAdapter<Number> {

    @Override
    public void write(JsonWriter out, Number value)
            throws IOException {
        out.value(value);
    }

    @Override
    public Number read(JsonReader in) throws IOException {
        if (in.peek() == JsonToken.NULL) {
            in.nextNull();
            return null;
        }
        try {
            String result = in.nextString();
            if ("".equals(result)) {
                return null;
            }
            return Integer.parseInt(result);
        } catch (NumberFormatException e) {
            throw new JsonSyntaxException(e);
        }
    }
}