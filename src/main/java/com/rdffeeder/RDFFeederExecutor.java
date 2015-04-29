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
                    "(stream with the given name will be created if it doesn't exist already).");
            return;
        }


        String rdfFilePath = args[0];
        String streamName = args[1];
        new RDFFeeder(streamName).feed(rdfFilePath);
    }

}
