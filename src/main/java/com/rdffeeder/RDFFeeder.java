package com.rdffeeder;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import polimi.deib.csparql_rest_api.RSP_services_csparql_API;
import polimi.deib.csparql_rest_api.exception.ServerErrorException;
import polimi.deib.csparql_rest_api.exception.StreamErrorException;

public class RDFFeeder {

    private static final Logger logger = LoggerFactory.getLogger(RDFFeeder.class);

    public static final String PROPERTY_FILE_NAME = "rdffeeder.properties";
    public static final String RDF_ENGINE_ENDPOINT_PROPERTY = "rdfengineEndpoint";

    private RSP_services_csparql_API rdfEngine;
    private String rdfStreamName;

    public RDFFeeder(String rdfStreamName) throws Exception {
        initConnectionWithRDFEngine();
        initRDFStream(rdfStreamName);
    }

    private void initRDFStream(String rdfStreamName) throws Exception {
        if (!rdfEngine.getStreamsInfo().contains(rdfStreamName)) {
            logger.info("RDF stream {} doesn't exist yet. Will register it.", rdfStreamName);
            rdfEngine.registerStream(rdfStreamName);
            logger.info("Sent registration request for stream {} to RDF engine. Will wait 60 seconds " +
                    "to give the RDF engine time to properly register the stream.", rdfStreamName);
            Thread.sleep(60000);
        } else {
            logger.info("RDF stream {} already exists.", rdfStreamName);
        }
        this.rdfStreamName = rdfStreamName;
    }

    private void initConnectionWithRDFEngine() throws ConfigurationException {
        PropertiesConfiguration properties = new PropertiesConfiguration(PROPERTY_FILE_NAME);
        rdfEngine = new RSP_services_csparql_API(properties.getString(RDF_ENGINE_ENDPOINT_PROPERTY));
    }

    public void feed(String rdfFilePath) throws ServerErrorException, StreamErrorException {
        logger.info("Reading in file {} for feeding.", rdfFilePath);
        Model rdfModel = ModelFactory.createDefaultModel();
        rdfModel.read(rdfFilePath);

        logger.info("Starting to feed the RDF engine.");
        rdfEngine.feedStream(rdfStreamName, rdfModel);
        logger.info("Feeding {} to stream {} successfully completed.", rdfFilePath, rdfStreamName);
    }
}
