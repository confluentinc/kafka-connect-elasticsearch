The integration-tests are built to run with `maven-surefire-plugin` in conjuction with 
`docker-maven-plugin` by `io.fabric8` to set up docker containers for Elasticsearch 
(embedded nodes are no longer supported or recommended by Elasticsearch).

To run the tests in their entirety, use `mvn verify`. This will setup, run and tear
down integration tests correctly (which `mvn integration-test` will not do). `mvn install` will
also run as `mvn-verify`, and additionally install the packages into the local repository.
`mvn package` and `mvn test` will run unit-tests but not integration-tests

To run the test manually (e.g. in Intellij IDEA),
* Set up container with `mvn pre-integration-test`.
* Run the tests manually
* Tear down container with `mvn post-integration-test` between each test to ensure a clean slate.

See the plugin specification in `pom.xml` for more details.

Some integration tests test HTTPS connections. For these certificates are generated
using `src/test/resources/certs/generate_certificates.sh`. These certificates are copied
into the elasticsearch docker image, and referenced in the connector config. They may need
to be regenerated if the certificates expire, or if the FQDN for either certificate changes 
(which may happen if the way Jenkins assigns hosts/IPs changes).
