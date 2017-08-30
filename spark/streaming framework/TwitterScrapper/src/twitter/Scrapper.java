package twitter;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class Scrapper {

	public static void main(String[] args) throws InterruptedException, ParseException {
		//Declaring the connection information:
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
		BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);

		/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
		List<Long> followings = Lists.newArrayList(1234L, 566788L);
		List<String> terms = Lists.newArrayList("#obama", "#trump");
		hosebirdEndpoint.followings(followings);
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1("p3OkG8bp2g5ks1qwylmaAArrb", "GRmsW4RLZbqtsKFK66NeGB6NhFsGTgtgqMCsVPHYOWuXZXKWXS", "2920199964-ZXhU2x0QMOiAC3tZbrAZ2pRp2MDAwuNAyP8QR4a", "xiKYh1rM8D1tjWXG4SoVRJhr3pNCYkYmKgUXh7Hh3yKcH");
		
		//Creating a client:
		ClientBuilder builder = new ClientBuilder()
		  .name("Hosebird-Client-01")                              // optional: mainly for the logs
		  .hosts(hosebirdHosts)
		  .authentication(hosebirdAuth)
		  .endpoint(hosebirdEndpoint)
		  .processor(new StringDelimitedProcessor(msgQueue))
		  .eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

		Client hosebirdClient = builder.build();
		// Attempts to establish a connection.
		hosebirdClient.connect();
		
		
		//Kafka Producer
		
		  Properties props = new Properties();
		  props.put("bootstrap.servers", "localhost:9092");
		  props.put("acks", "all");
		  props.put("retries", 0);
		  props.put("batch.size", 16384);
		  props.put("buffer.memory", 33554432);
		  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		  Producer<String, String> producer = new KafkaProducer(props);


		
		while (!hosebirdClient.isDone()) {
			  String msg = msgQueue.take();
			  JSONParser jp = new JSONParser();
			  JSONObject json = (JSONObject) jp.parse(msg);
			 if(!json.containsKey("retweeted_status"))
			 {
			  String textString = (String) json.get("text");
			  //System.out.println(msg);
			  producer.send(new ProducerRecord<String, String>("tw", "tw", textString));
			  //System.out.println("----------------------------------------------------");
			 }
			}
	}

}
