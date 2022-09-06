#!/bin/sh -e

sslBasePath="$1"
# Since these certs are only for local and test environments, we will use the same password everywhere
localSslPassword="dev_pass"

# PKCS12 Keystore
pkcs12KeystoreFile="${sslBasePath}/localhost.p12"
pkcs12KeystorePassword="${localSslPassword}"

# JKS Keystore
jksKeystoreFile="${sslBasePath}/localhost.jks"
jksKeystorePassword="${localSslPassword}"

# Cert for "localhost"
localhostPrivateKeyFile="${sslBasePath}/localhost.key"
localhostPkcs12KeystoreAlias="selfsigned"
localhostPrivateKeyPassword="${localSslPassword}"
localhostCertFile="${sslBasePath}/localhost.cert"
localhostDN="CN=localhost"

# Cert for "dummy"
dummyPrivateKeyFile="${sslBasePath}/dummy.key"
dummyPkcs12KeystoreAlias="selfsigned_dummy"
dummyPrivateKeyPassword="${localSslPassword}"
dummyCertFile="${sslBasePath}/dummy.cert"
dummyDN="CN=dummy"

mkdir -p "${sslBasePath}"

rm -f "${pkcs12KeystoreFile}" "${jksKeystoreFile}" "${localhostPrivateKeyFile}" "${localhostCertFile}" "${dummyPrivateKeyFile}" "${dummyCertFile}"

# generate a 10 year self-signed cert and key
keytool -genkey -keyalg RSA -sigalg SHA512withRSA -alias "${localhostPkcs12KeystoreAlias}" -keystore "${pkcs12KeystoreFile}" -storetype PKCS12 -storepass "${pkcs12KeystorePassword}" -validity 3650 -keysize 2048 -dname "${localhostDN}"

# extract .cert and .key files
openssl pkcs12 -in "${pkcs12KeystoreFile}" -nokeys -passin "pass:${localhostPrivateKeyPassword}" -password "pass:${pkcs12KeystorePassword}" | sed -ne '/-BEGIN CERTIFICATE-/,/-END CERTIFICATE-/p' > "${localhostCertFile}"
openssl pkcs12 -in "${pkcs12KeystoreFile}" -nodes -nocerts -passin "pass:${localhostPrivateKeyPassword}" -password "pass:${pkcs12KeystorePassword}" | sed -ne '/-BEGIN PRIVATE KEY-/,/-END PRIVATE KEY-/p' > "${localhostPrivateKeyFile}"

## generate another 10 year self-signed cert and key to mimic an environment where  keystore has multiple certs
keytool -genkey -keyalg RSA -sigalg SHA512withRSA -alias "${dummyPkcs12KeystoreAlias}" -keystore "${pkcs12KeystoreFile}" -storetype PKCS12 -storepass "${pkcs12KeystorePassword}" -validity 3650 -keysize 2048 -dname "${dummyDN}"

# convert the PKCS12 .p12 file to a JKS file
keytool -importkeystore -srckeystore "${pkcs12KeystoreFile}" -srcstorepass "${pkcs12KeystorePassword}" -srcstoretype pkcs12 -destkeystore "${jksKeystoreFile}" -deststoretype jks -deststorepass "${jksKeystorePassword}"