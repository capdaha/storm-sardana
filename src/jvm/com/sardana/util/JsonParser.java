package com.sardana.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

import java.io.IOException;

/**
 * @author Sardana
 */
public class JsonParser {

    private ObjectReader reader;

    public JsonParser(Class c) {
        ObjectMapper mapper = new ObjectMapper();
        reader = mapper.reader(c);
    }

    public Object parseJson(String json) throws IOException {
        return reader.readValue(json);
    }
}