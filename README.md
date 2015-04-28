# rdf-feeder
Executable Java application to feed RDF-s to C-SPARQL engine

Dependencies are Java 7 and a running [C-SPARQL engine](https://github.com/a71993/csparqlpush/tree/master/rsp_services_csparql).

## Setup

* Modify src/main/resources/rdffeeder.properties to match your environment.
* Run `./gradlew clean build fatJar` and the application will be built to build/libs/rdf-feeder-[version].jar
* Run the application `java -jar rdf-feeder-[version].jar [path to the RDF file to be sent to the RDF engine] [stream name]` (Stream with the given name will be created if it doesn't exist already).