import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Consumer {

    public static void main(String[] args) {
    	
//    	 String topic = args[0].toString();
//         String group = args[1].toString();
         Properties props = new Properties();
         props.put("bootstrap.servers", "localhost:9092");
         props.put("group.id", "my-group");
         props.put("enable.auto.commit", "true");
         props.put("auto.commit.interval.ms", "1000");
         props.put("session.timeout.ms", "30000");
         props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
         props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
         props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
         props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
         KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
         
         consumer.subscribe(Arrays.asList("tw"));
        // System.out.println("Subscribed to topic " + topic);
		NLP.init();
		
		while(true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for(ConsumerRecord<String, String> record : records){
				String location = Consumer.getRandomLocation();
				String post_date = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new Date());
				int sentiment = NLP.findSentiment(record.value());
				String sentiString = "";
				if(sentiment == 1)
				{
					sentiString = "sad";
				}
				else if(sentiment == 2)
				{
					sentiString = "Neutral";
				}
				else if(sentiment == 3)
				{
					sentiString = "Happy";
				}
					
				System.out.printf("[location = %s, Sentiment = %s, Tweet = %s, Date = %s]\n", 
					location , sentiString, record.value(), post_date);
			}
		}
    }
    
    private static String getRandomLocation() {
		List<String> places = Arrays.asList("TX", "CA", "TN", "NY", "IL", "IA"); 
		  Random randomGenerator = new Random();
		int randomInt = randomGenerator.nextInt(5);
		return places.get(randomInt);
	}
}