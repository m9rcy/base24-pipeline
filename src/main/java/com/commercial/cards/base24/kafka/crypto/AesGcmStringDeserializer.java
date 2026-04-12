package com.commercial.cards.base24.kafka.crypto;

import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class AesGcmStringDeserializer implements Deserializer<String> {

    private AesGcmStringCrypto crypto;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.crypto = new AesGcmStringCrypto(configs);
    }

    @Override
    public String deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        String value = new String(data, StandardCharsets.UTF_8);
        return crypto.decryptIfEncrypted(value);
    }
}
