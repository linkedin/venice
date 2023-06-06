package com.linkedin.venice.authentication.jwt;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwsHeader;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.SigningKeyResolver;
import io.jsonwebtoken.io.Decoders;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URL;
import java.security.Key;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.RSAPublicKeySpec;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class JwksUriSigningKeyResolver implements SigningKeyResolver {
  private static final Logger log = LogManager.getLogger(JwksUriSigningKeyResolver.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final String algorithm;
  private final Pattern hostsAllowlist;
  private final Key fallbackKey;
  private volatile Map<String, Key> keyMap = new ConcurrentHashMap<>();

  public JwksUriSigningKeyResolver(String algorithm, String hostsAllowlist, Key fallbackKey) {
    this.algorithm = algorithm;
    if (StringUtils.isBlank(hostsAllowlist)) {
      this.hostsAllowlist = null;
    } else {
      this.hostsAllowlist = Pattern.compile(hostsAllowlist);
    }
    this.fallbackKey = fallbackKey;
  }

  @Override
  public Key resolveSigningKey(JwsHeader header, Claims claims) {
    final String jwksUri = (String) claims.get("jwks_uri");
    if (jwksUri == null) {
      return fallbackKey;
    }
    return getKey(jwksUri);
  }

  @Override
  public Key resolveSigningKey(JwsHeader header, String plaintext) {
    throw new UnsupportedOperationException();
  }

  private Key getKey(String uri) {
    return keyMap.computeIfAbsent(uri, this::fetchKey);
  }

  private Key fetchKey(String uri) {
    try {
      final URL src = new URL(uri);
      if (hostsAllowlist == null || !hostsAllowlist.matcher(src.getHost()).matches()) {
        throw new JwtException("Untrusted hostname: '" + src.getHost() + "'");
      }
      final JwkKeys keys = MAPPER.readValue(src, JwkKeys.class);
      for (JwkKey key: keys.getKeys()) {
        if (!algorithm.equals(key.getAlg())) {
          continue;
        }
        BigInteger modulus = base64ToBigInteger(key.getN());
        BigInteger exponent = base64ToBigInteger(key.getE());
        RSAPublicKeySpec rsaPublicKeySpec = new RSAPublicKeySpec(modulus, exponent);
        try {
          KeyFactory keyFactory = KeyFactory.getInstance("RSA");
          return keyFactory.generatePublic(rsaPublicKeySpec);
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
          throw new IllegalStateException("Failed to parse public key");
        }
      }
      throw new JwtException("No valid keys found from URL: " + uri);
    } catch (IOException e) {
      System.out.println(e);
      log.error("Failed to fetch keys from URL: {}", uri, e);
      throw new JwtException("Failed to fetch keys from URL: " + uri, e);
    }
  }

  private static class JwkKeys {
    private final List<JwkKey> keys;

    public JwkKeys(List<JwkKey> keys) {
      this.keys = keys;
    }

    public List<JwkKey> getKeys() {
      return keys;
    }
  }

  private static class JwkKey {
    private final String alg;
    private final String e;
    private final String kid;
    private final String kty;
    private final String n;

    public JwkKey(String alg, String e, String kid, String kty, String n) {
      this.alg = alg;
      this.e = e;
      this.kid = kid;
      this.kty = kty;
      this.n = n;
    }

    public String getAlg() {
      return alg;
    }

    public String getE() {
      return e;
    }

    public String getKid() {
      return kid;
    }

    public String getKty() {
      return kty;
    }

    public String getN() {
      return n;
    }
  }

  private BigInteger base64ToBigInteger(String value) {
    return new BigInteger(1, Decoders.BASE64URL.decode(value));
  }
}
