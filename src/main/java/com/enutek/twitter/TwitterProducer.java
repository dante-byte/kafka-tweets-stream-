package com.enutek.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import io.github.cdimascio.dotenv.Dotenv;


public class TwitterProducer {

    private Dotenv dotenv = Dotenv.configure().load();
    private String consumerKey = dotenv.get("consumerKey");
    private String consumerSecret = dotenv.get("consumerSecret") ;
    private String token = dotenv.get("token");
    private String secret = dotenv.get("secret");

    public TwitterProducer() {

    }

    public void run() {


    }

    public void createTwitterClient() {

        /** Set up your blocking queues:
         * Be sure to size these properly
         * based on expected TPS of your
         * stream */

        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);


        /** Declare the host you want to connect to,
         the endpoint, and authentication (basic auth
         or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        List<String> terms = Lists.newArrayList("kafka");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

    }

    public static void main(String[] args) {

    }
}
