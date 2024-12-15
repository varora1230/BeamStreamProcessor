package org.data.encoder;

import org.apache.beam.sdk.coders.CustomCoder;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.data.pojo.Person;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class PersonCoder extends CustomCoder<Person> {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Person decode(InputStream inStream) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        byte[] temp = new byte[1024];
        int bytesRead;
        while ((bytesRead = inStream.read(temp)) != -1) {
            buffer.write(temp, 0, bytesRead);
        }
        byte[] bytes = buffer.toByteArray();
        return objectMapper.readValue(bytes, Person.class);
    }

    @Override
    public void encode(Person value, OutputStream outStream) throws IOException {
        String json = objectMapper.writeValueAsString(value);
        outStream.write(json.getBytes());
    }
}
