package com.commercial.cards.base24.kafka.crypto;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Map;

final class AesGcmStringCrypto {

    static final String PREFIX = "aes-gcm-v1:";
    private static final String KEY_BASE64_CONFIG = "base24.crypto.key-base64";
    private static final String KEY_TEXT_CONFIG = "base24.crypto.key";
    private static final String KEY_BASE64_ENV = "KAFKA_SYMMETRIC_KEY_BASE64";
    private static final String KEY_TEXT_ENV = "KAFKA_SYMMETRIC_KEY";
    private static final int IV_BYTES = 12;
    private static final int TAG_BITS = 128;

    private final SecureRandom secureRandom = new SecureRandom();
    private final SecretKeySpec key;

    AesGcmStringCrypto(Map<String, ?> configs) {
        this.key = new SecretKeySpec(resolveKey(configs), "AES");
    }

    String encrypt(String plaintext) {
        try {
            byte[] iv = new byte[IV_BYTES];
            secureRandom.nextBytes(iv);

            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(TAG_BITS, iv));
            byte[] ciphertext = cipher.doFinal(plaintext.getBytes(StandardCharsets.UTF_8));

            Base64.Encoder encoder = Base64.getEncoder();
            return PREFIX + encoder.encodeToString(iv) + ":" + encoder.encodeToString(ciphertext);
        } catch (GeneralSecurityException ex) {
            throw new IllegalStateException("Failed to encrypt Kafka message", ex);
        }
    }

    String decryptIfEncrypted(String value) {
        if (value == null || !value.startsWith(PREFIX)) {
            return value;
        }

        String encrypted = value.substring(PREFIX.length());
        int separator = encrypted.indexOf(':');
        if (separator < 0) {
            throw new IllegalArgumentException("Encrypted Kafka message is missing IV separator");
        }

        try {
            Base64.Decoder decoder = Base64.getDecoder();
            byte[] iv = decoder.decode(encrypted.substring(0, separator));
            byte[] ciphertext = decoder.decode(encrypted.substring(separator + 1));

            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(TAG_BITS, iv));
            byte[] plaintext = cipher.doFinal(ciphertext);
            return new String(plaintext, StandardCharsets.UTF_8);
        } catch (GeneralSecurityException | IllegalArgumentException ex) {
            throw new IllegalArgumentException("Failed to decrypt Kafka message", ex);
        }
    }

    private byte[] resolveKey(Map<String, ?> configs) {
        String base64Key = firstNonBlank(configValue(configs, KEY_BASE64_CONFIG), System.getenv(KEY_BASE64_ENV));
        if (base64Key != null) {
            byte[] decoded = Base64.getDecoder().decode(base64Key);
            validateKeyLength(decoded);
            return decoded;
        }

        String textKey = firstNonBlank(configValue(configs, KEY_TEXT_CONFIG), System.getenv(KEY_TEXT_ENV));
        if (textKey == null) {
            throw new IllegalArgumentException("Set " + KEY_BASE64_ENV + " or " + KEY_TEXT_ENV
                    + " for AES-GCM Kafka message encryption");
        }

        byte[] bytes = textKey.getBytes(StandardCharsets.UTF_8);
        validateKeyLength(bytes);
        return bytes;
    }

    private String configValue(Map<String, ?> configs, String name) {
        Object value = configs.get(name);
        return value == null ? null : value.toString();
    }

    private String firstNonBlank(String first, String second) {
        if (first != null && !first.isBlank()) {
            return first;
        }
        if (second != null && !second.isBlank()) {
            return second;
        }
        return null;
    }

    private void validateKeyLength(byte[] keyBytes) {
        int length = keyBytes.length;
        if (length != 16 && length != 24 && length != 32) {
            throw new IllegalArgumentException("AES key must be 16, 24, or 32 bytes, but was " + length);
        }
    }
}
