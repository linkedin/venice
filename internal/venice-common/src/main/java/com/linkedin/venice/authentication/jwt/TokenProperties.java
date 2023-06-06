package com.linkedin.venice.authentication.jwt;

/**
 * Token properties
 * secretKey secret key when using symmetric keys or private key when using asymmetric keys
 * publicKey public key when using asymmetric keys
 * authClaim claim from which to extract the authentication role (default "sub")
 * publicAlg algorithm used to sign the token (default "HS256")
 * audienceClaim claim from which to extract the audience (default null, no audience check)
 * audience audience to verify
 * adminRoles list of roles that are considered as admin
 */
public class TokenProperties {
  private final String secretKey;
  private final String publicKey;
  private final String authClaim;
  private final String publicAlg;
  private final String audienceClaim;
  private final String audience;
  private final String jwksHostsAllowlist;

  public TokenProperties(
      String secretKey,
      String publicKey,
      String authClaim,
      String publicAlg,
      String audienceClaim,
      String audience,
      String jwksHostsAllowlist) {
    this.secretKey = secretKey;
    this.publicKey = publicKey;
    this.authClaim = authClaim;
    this.publicAlg = publicAlg;
    this.audienceClaim = audienceClaim;
    this.audience = audience;
    this.jwksHostsAllowlist = jwksHostsAllowlist;
  }

  public String getSecretKey() {
    return secretKey;
  }

  public String getPublicKey() {
    return publicKey;
  }

  public String getAuthClaim() {
    return authClaim;
  }

  public String getPublicAlg() {
    return publicAlg;
  }

  public String getAudienceClaim() {
    return audienceClaim;
  }

  public String getAudience() {
    return audience;
  }

  public String getJwksHostsAllowlist() {
    return jwksHostsAllowlist;
  }

  @Override
  public String toString() {
    return "TokenProperties{" + "secretKey='" + secretKey + '\'' + ", publicKey='" + publicKey + '\'' + ", authClaim='"
        + authClaim + '\'' + ", publicAlg='" + publicAlg + '\'' + ", audienceClaim='" + audienceClaim + '\''
        + ", audience='" + audience + "', jwksHostsAllowlist='" + jwksHostsAllowlist + '\'' + '}';
  }

}
