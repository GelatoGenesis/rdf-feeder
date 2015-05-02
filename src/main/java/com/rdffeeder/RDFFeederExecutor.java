package com.rdffeeder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Date;

public class RDFFeederExecutor {

    private static final Logger logger = LoggerFactory.getLogger(RDFFeederExecutor.class);

    public static void main(String[] args) throws Exception {
        logger.info("RDF stream feeder invoked @ " + new Date() + " with params " + Arrays.asList(args));
        if(args.length < 2) {

            logger.info("RDF streamer takes two mandatory parameters. First one is the " +
                    "path to the RDF file to be sent and the second one is the stream name " +
                    "(stream with the given name will be created if it doesn't exist already). " +
                    "Third parameter is optional. It represents the mode in which the feeder works. " +
                    "Default mode will assume that the input file is valid XML with a single root RDF element. " +
                    "Mode 2 assumes that the file doesnt have <?xml ... header and may contain multiple " +
                    "RDF elements.");
            return;
        }

        String rdfFilePath = args[0];
        String streamName = args[1];

        RDFFeeder rdfFeeder = new RDFFeeder(streamName);

        RDFFeederExecutionRegime regime = RDFFeederExecutionRegime.FEED_VALID_XML_FILE_WITH_SINGLE_RDF; // default regime if one is not given as input.

        if (args.length >= 3) {
            regime = RDFFeederExecutionRegime.fromString(args[2]);
        }

        switch (regime) {
            case MULTIPLE_ROOT_RDF_ELEMENTS_XML_HEADER_MISSING:
                rdfFeeder.feedMultiRootFile(rdfFilePath);
                break;
            default:
                rdfFeeder.feedFile(rdfFilePath);
        }
        if (regime == RDFFeederExecutionRegime.FEED_VALID_XML_FILE_WITH_SINGLE_RDF) {
            rdfFeeder.feedFile(rdfFilePath);
        }

    }

    enum RDFFeederExecutionRegime {
        FEED_VALID_XML_FILE_WITH_SINGLE_RDF(1),
        MULTIPLE_ROOT_RDF_ELEMENTS_XML_HEADER_MISSING(2);

        private  int regimeAsint;

        RDFFeederExecutionRegime(int regimeAsInt) {
            this.regimeAsint = regimeAsInt;
        }

        static RDFFeederExecutionRegime fromInt(int regimeAsint) {
            for (RDFFeederExecutionRegime regime : RDFFeederExecutionRegime.values()) {
                if (regime.regimeAsint == regimeAsint) {
                    return regime;
                }
            }
            throw new RuntimeException("Invalid regime: " + regimeAsint);
        }

        static RDFFeederExecutionRegime fromString(String regimeNo) {
            try {
                return fromInt(Integer.parseInt(regimeNo));
            } catch (Exception e) {
                throw new RuntimeException("Invalid regime: " + regimeNo);
            }
        }
    }

}
