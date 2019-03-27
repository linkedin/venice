# Venice Development Keys

The keys and certificates in this folder were generated in 2017 with a 10 year expiration.  That means any automated tests that rely on these certificates will start failing sometime in 2027. These keys are intended to be used as development keys and for automated unit tests.

The password is `dev_pass`

localhost.cert should also be used as the trust store since it is a self-signed cert

 * localhost.p12 - PKCS12 keystore
 * localhost.jks - JKS keystore
 * localhost.cert - self-signed certificate
 * localhost.key - private key

This is the process used to generate these files.  Note, run these commands one at a time because some of them trigger interactive prompts.

```bash
# generate 10 year self-signed cert and key
# "first and last name" should be filled out as "localhost"
keytool -genkey -keyalg RSA -sigalg SHA512withRSA -alias selfsigned -keystore localhost.p12 -storetype PKCS12 -storepass dev_pass -validity 3650 -keysize 2048

# extract .cert and .key files
openssl pkcs12 -in localhost.p12 -nokeys -out localhost.cert
openssl pkcs12 -in localhost.p12 -nodes -nocerts -out localhost.key
# manually delete the header from the output files, leave ---BEGIN CERT--- or ---BEGIN PRIVATE KEY--- to the end

# conver the PKCS12 .p12 file to a JKS file
keytool -importkeystore -srckeystore localhost.p12 -srcstoretype pkcs12 -srcalias selfsigned -destkeystore localhost.jks -deststoretype jks -deststorepass dev_pass -destalias selfsigned
```
