package com.commercial.cards.base24.kafka.crypto;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AesGcmStringSerdeTest {

    @Test
    void shouldEncryptAndDecryptStringMessages() {
        Map<String, ?> configs = Map.of("base24.crypto.key", "0123456789abcdef");
        AesGcmStringSerializer serializer = new AesGcmStringSerializer();
        AesGcmStringDeserializer deserializer = new AesGcmStringDeserializer();
        serializer.configure(configs, false);
        deserializer.configure(configs, false);

        byte[] encrypted = serializer.serialize("topic", "<Data/>");
        String encryptedText = new String(encrypted, StandardCharsets.UTF_8);

        assertTrue(encryptedText.startsWith(AesGcmStringCrypto.PREFIX));
        assertNotEquals("<Data/>", encryptedText);
        assertEquals("<Data/>", deserializer.deserialize("topic", encrypted));
    }

    @Test
    void shouldPassPlaintextThroughDeserializer() {
        AesGcmStringDeserializer deserializer = new AesGcmStringDeserializer();
        deserializer.configure(Map.of("base24.crypto.key", "0123456789abcdef"), false);

        assertEquals("<Data/>", deserializer.deserialize("topic", "<Data/>".getBytes(StandardCharsets.UTF_8)));
    }
}
