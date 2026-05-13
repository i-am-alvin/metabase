# Mongo server with SSL support

## Running the server

The script `run-server.sh` can be used to run the metabase-qa Mongo server in
docker. It is possible to run different versions of Mongo with different SSL
configurations (plain, ssl and tls).

## Keys and certificates

When the docker container starts, the client key and the client and CA
certificates needed for the backend tests are copied into the this directory.

Although the source of truth is the docker image, the client certificates are
checked in because there are tests like the SSL connection factory tests which
need valid certificates and keys.

## Testing with X509 client certs

The `metabase.driver.mongo-test/can-connect?-with-x509` test is commented out
because it can't run in CI due to needing this docker image instead of the
regular one. To manually test it, start `run-server.sh`, then run this command
to create the user needed for the X509 client certificate:

    docker run --network=container:metamongo -it metabase/qa-databases:mongo-sample-5 \
      mongosh --username metabase --password metasample123 \
              --tls --tlsCertificateKeyFile /etc/mongo/metamongo.pem \
              --tlsCAFile /etc/mongo/metaca.crt --host localhost \
              --eval 'db.getSiblingDB("$external").runCommand(
                {
                  createUser: "emailAddress=metabase@localhost,CN=localhost,OU=metabase,O=Metabase Inc.,L=San Francisco,ST=CA,C=US",
                  roles: [
                       { role: "readWrite", db: "test-data" },
                       { role: "userAdminAnyDatabase", db: "admin" }
                  ],
                  writeConcern: { w: "majority" , wtimeout: 5000 }
                }
              ) '
