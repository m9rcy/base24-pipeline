package com.commercial.cards.base24.kafka.crypto;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

    @Test
    void shouldReturnNullWhenSerializingNullData() {
        AesGcmStringSerializer serializer = new AesGcmStringSerializer();
        serializer.configure(Map.of("base24.crypto.key", "0123456789abcdef"), false);

        assertNull(serializer.serialize("topic", null));
    }

    @Test
    void shouldReturnNullWhenDeserializingNullData() {
        AesGcmStringDeserializer deserializer = new AesGcmStringDeserializer();
        deserializer.configure(Map.of("base24.crypto.key", "0123456789abcdef"), false);

        assertNull(deserializer.deserialize("topic", null));
    }

    @Test
    void shouldAcceptBase64EncodedKey() {
        // "0123456789abcdef" base64 encoded
        Map<String, ?> configs = Map.of("base24.crypto.key-base64", "MDEyMzQ1Njc4OWFiY2RlZg==");
        AesGcmStringSerializer serializer = new AesGcmStringSerializer();
        AesGcmStringDeserializer deserializer = new AesGcmStringDeserializer();
        serializer.configure(configs, false);
        deserializer.configure(configs, false);

        byte[] encrypted = serializer.serialize("topic", "hello");
        assertEquals("hello", deserializer.deserialize("topic", encrypted));
    }

    @Test
    void shouldThrowWhenNoKeyIsConfigured() {
        assertThrows(IllegalArgumentException.class,
                () -> new AesGcmStringSerializer().configure(Map.of(), false));
    }

    @Test
    void shouldThrowWhenKeyLengthIsInvalid() {
        // 10 bytes — not a valid AES key size
        assertThrows(IllegalArgumentException.class,
                () -> new AesGcmStringSerializer().configure(Map.of("base24.crypto.key", "tooshort"), false));
    }

    @Test
    void shouldThrowWhenEncryptedMessageIsMissingIvSeparator() {
        AesGcmStringDeserializer deserializer = new AesGcmStringDeserializer();
        deserializer.configure(Map.of("base24.crypto.key", "0123456789abcdef"), false);

        // Valid prefix but no IV:ciphertext separator
        String malformed = AesGcmStringCrypto.PREFIX + "nocolon";
        assertThrows(IllegalArgumentException.class,
                () -> deserializer.deserialize("topic", malformed.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void shouldAccept24ByteKey() {
        // 24 ASCII chars = 24 bytes = 192-bit AES key
        Map<String, ?> configs = Map.of("base24.crypto.key", "0123456789abcdefghijklmn");
        AesGcmStringSerializer serializer = new AesGcmStringSerializer();
        AesGcmStringDeserializer deserializer = new AesGcmStringDeserializer();
        serializer.configure(configs, false);
        deserializer.configure(configs, false);

        byte[] encrypted = serializer.serialize("topic", "hello");
        assertEquals("hello", deserializer.deserialize("topic", encrypted));
    }

    @Test
    void shouldAccept32ByteKey() {
        // 32 ASCII chars = 32 bytes = 256-bit AES key
        Map<String, ?> configs = Map.of("base24.crypto.key", "0123456789abcdef0123456789abcdef");
        AesGcmStringSerializer serializer = new AesGcmStringSerializer();
        AesGcmStringDeserializer deserializer = new AesGcmStringDeserializer();
        serializer.configure(configs, false);
        deserializer.configure(configs, false);

        byte[] encrypted = serializer.serialize("topic", "hello");
        assertEquals("hello", deserializer.deserialize("topic", encrypted));
    }

    @Test
    void shouldThrowWhenDecryptingTamperedCiphertext() {
        Map<String, ?> configs = Map.of("base24.crypto.key", "0123456789abcdef");
        AesGcmStringSerializer serializer = new AesGcmStringSerializer();
        AesGcmStringDeserializer deserializer = new AesGcmStringDeserializer();
        serializer.configure(configs, false);
        deserializer.configure(configs, false);

        byte[] encrypted = serializer.serialize("topic", "hello");
        String encryptedStr = new String(encrypted, StandardCharsets.UTF_8);

        // Corrupt the last character of the base64 ciphertext to break the GCM auth tag
        int last = encryptedStr.length() - 1;
        char replacement = encryptedStr.charAt(last) == 'A' ? 'B' : 'A';
        String tampered = encryptedStr.substring(0, last) + replacement;

        assertThrows(IllegalArgumentException.class,
                () -> deserializer.deserialize("topic", tampered.getBytes(StandardCharsets.UTF_8)));
    }
}
