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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileReader;
import java.nio.charset.StandardCharsets;

public class RDFFeeder {

    private static final Logger logger = LoggerFactory.getLogger(RDFFeeder.class);

    public static final String PROPERTY_FILE_NAME = "rdffeeder.properties";
    public static final String RDF_ENGINE_ENDPOINT_PROPERTY = "rdfengineEndpoint";
    public static final String XML_HEADER_TAG = "<?xml version=\"1.0\"?>";
    public static final String RDF_ROOT_END_TAG = "</rdf:RDF>";

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
        } else {
            logger.info("RDF stream {} already exists.", rdfStreamName);
        }
        this.rdfStreamName = rdfStreamName;
    }

    private void initConnectionWithRDFEngine() throws ConfigurationException {
        PropertiesConfiguration properties = new PropertiesConfiguration(PROPERTY_FILE_NAME);
        rdfEngine = new RSP_services_csparql_API(properties.getString(RDF_ENGINE_ENDPOINT_PROPERTY));
    }

    public void feedFile(String rdfFilePath) throws ServerErrorException, StreamErrorException {
        logger.info("Reading in file {} for feeding.", rdfFilePath);
        Model rdfModel = ModelFactory.createDefaultModel();
        rdfModel.read(rdfFilePath);

        logger.info("Starting to feedFile the RDF engine.");
        rdfEngine.feedStream(rdfStreamName, rdfModel);
        logger.info("Feeding {} to stream {} successfully completed.", rdfFilePath, rdfStreamName);
    }

    public void feedString(String rdf) throws Exception {
        logger.info("Starting to feed RDF from a string to stream {}.", rdfStreamName);
        Model rdfModel = ModelFactory.createDefaultModel();
        rdfModel.read(new ByteArrayInputStream(rdf.getBytes(StandardCharsets.UTF_8)), "");

        rdfEngine.feedStream(rdfStreamName, rdfModel);
        logger.info("Feeding RDF string to stream {} successfully completed.", rdfStreamName);
    }


    public void feedMultiRootFile(String rdfFilePath) throws Exception {
        logger.info("Starting to process and feed a multiroot file which has no xml tag at the beginning.");
        try (BufferedReader br = new BufferedReader(new FileReader(rdfFilePath))) {
            StringBuilder rdf = new StringBuilder();
            for(String line; (line = br.readLine()) != null; ) {
                if (rdf.length() == 0) {
                    rdf.append(XML_HEADER_TAG);
                }
                rdf.append(line);
                if (line.contains(RDF_ROOT_END_TAG)) {
                    feedString(rdf.toString());
                    rdf = new StringBuilder();
                }
            }
        }
    }
}
