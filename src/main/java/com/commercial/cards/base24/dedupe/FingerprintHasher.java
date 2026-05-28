package com.commercial.cards.base24.dedupe;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Optional;

public class FingerprintHasher {

    private static final String HMAC_ALGORITHM = "HmacSHA256";

    private final ObjectMapper objectMapper;
    private final Optional<byte[]> hmacSecret;

    public FingerprintHasher(ObjectMapper objectMapper, String hmacSecret) {
        this.objectMapper = objectMapper.copy()
                .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
                .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
        this.hmacSecret = hmacSecret == null || hmacSecret.isBlank()
                ? Optional.empty()
                : Optional.of(hmacSecret.getBytes(StandardCharsets.UTF_8));
    }

    public String hash(Object fingerprint) {
        try {
            byte[] canonical = objectMapper.writeValueAsBytes(fingerprint);
            byte[] digest = hmacSecret
                    .map(secret -> hmacSha256(secret, canonical))
                    .orElseGet(() -> sha256(canonical));
            return HexFormat.of().formatHex(digest);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Unable to serialize event fingerprint", e);
        }
    }

    private byte[] sha256(byte[] canonical) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(canonical);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }

    private byte[] hmacSha256(byte[] secret, byte[] canonical) {
        try {
            Mac mac = Mac.getInstance(HMAC_ALGORITHM);
            mac.init(new SecretKeySpec(secret, HMAC_ALGORITHM));
            return mac.doFinal(canonical);
        } catch (Exception e) {
            throw new IllegalStateException("Unable to HMAC event fingerprint", e);
        }
    }
}
