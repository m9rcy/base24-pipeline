package com.commercial.cards.base24.kafka.crypto;

import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class AesGcmStringSerializer implements Serializer<String> {

    private AesGcmStringCrypto crypto;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.crypto = new AesGcmStringCrypto(configs);
    }

    @Override
    public byte[] serialize(String topic, String data) {
        if (data == null) {
            return null;
        }
        return crypto.encrypt(data).getBytes(StandardCharsets.UTF_8);
    }
}
