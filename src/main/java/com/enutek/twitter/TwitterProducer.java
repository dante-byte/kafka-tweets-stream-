package com.enutek.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import io.github.cdimascio.dotenv.Dotenv;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TwitterProducer {


    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
    private Dotenv dotenv = Dotenv.configure().load();

    private String consumerKey = dotenv.get("consumerKey");
    private String consumerSecret = dotenv.get("consumerSecret") ;
    private String token = dotenv.get("token");
    private String secret = dotenv.get("secret");

    public TwitterProducer() {

    }

    public static void main(String[] args) {

        new TwitterProducer().run();

    }

    public void run() {

        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000); /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        Client client = createTwitterClient(msgQueue);
        client.connect();


        /**
         * kafka producer
         */

        KafkaProducer<String, String> producer = createKafkaProducer();


        /**
         * loop to send tweets on a different thread, or multiple different threads
         */

        // \\....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null){

                logger.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                        if (e != null) {

                            logger.error("error");
                        }
                    }
                });


            }
            logger.info("end of application ");
        }


    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {


        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST); /** Declare the host you want to connect to, the endpoint, and authentication (basic author oauth) */

        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        List<String> terms = Lists.newArrayList("bitcoin");

        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);


        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")   // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;


    }

    public KafkaProducer<String,String> createKafkaProducer() {

        String bootstrapServers = "127.0.0.1:9092";
        //producer properties to passed
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // what type of value you're send to kafka in bytes // string serializer needed // key will be a string serialzier
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);


        return producer;



    }


}
