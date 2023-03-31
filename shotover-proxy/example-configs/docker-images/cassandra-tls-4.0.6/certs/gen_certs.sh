#!/bin/bash

set -e

help()
{
   echo "Generate keys for the Cassandra docker-compose"
   echo
   echo "Syntax: gen_certs [-o]"
   echo "options:"
   echo "o     Overwrite the existing keyfiles"
   echo "c     Clear the existing keyfiles"
   echo
}

clear()
{
   rm -f *.p12 *.jks *.key *.csr *.srl *.crt
}

OVERWRITE=false

while getopts ":hoc" option; do
   case $option in
      h) # display Help
         help
         exit;;
      o)
          OVERWRITE=true;;
      c)
         clear
         exit;;
      \?)
         echo "Error: Invalid option"
         exit;;
   esac
done

SCRIPT_DIR="$(cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$SCRIPT_DIR"

KEYSTORE=keystore.p12
if [ -f "$KEYSTORE" ] && [ "$OVERWRITE" = false ]; then
    echo "$KEYSTORE and $TRUSTSTORE already exist. Use -o to overwrite them."
    exit 0
fi

clear

# Generate localhost_CA and localhost certs/keys
openssl genrsa -out localhost_CA.key 4096
openssl req -x509 -new -config localhost_CA.cfg -key localhost_CA.key -days 9999 -out localhost_CA.crt
openssl genrsa -out localhost.key 4096
openssl req -new -config localhost.cfg -key localhost.key -days 9999 -out localhost.csr
openssl x509 -req -in localhost.csr -CA localhost_CA.crt -CAkey localhost_CA.key -CAcreateserial -days 9999 -out localhost.crt

# generate keystore
openssl pkcs12 -export -out keystore.p12 -inkey localhost.key -in localhost.crt -passout pass:password
keytool -importkeystore -destkeystore keystore.jks -srcstoretype PKCS12 -srckeystore keystore.p12 -deststorepass "password" -srcstorepass "password"
chmod o+rwx keystore.p12

echo "finished generating certs"
