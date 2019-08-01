Some integration tests test HTTPS connections. For these certificates are generated
using `src/test/resources/certs/generate_certificates.sh`. These certificates are copied
into the elasticsearch docker image, and referenced in the connector config. They may need
to be regenerated if the certificates expire, or if the FQDN for either certificate changes 
(which may happen if the way Jenkins assigns hosts/IPs changes).
