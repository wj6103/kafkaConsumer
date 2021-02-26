import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.json.JSONObject;

import java.io.*;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumer {
    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        String brokers = "ai-worker-01.silkrode.com.tw:9092,ai-worker-02.silkrode.com.tw:9092,ai-worker-03.silkrode.com.tw:9092,ai-worker-04.silkrode.com.tw:9092,ai-worker-05.silkrode.com.tw:9092,ai-worker-06.silkrode.com.tw:9092";
        properties.setProperty("bootstrap.servers", brokers);
        properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("group.id","test");
        Consumer<String,String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList("classifier"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ZERO);
            for (ConsumerRecord<String, String> record : records){
                JSONObject jsonObject = new JSONObject(record.value());
                String callback = jsonObject.get("callback").toString();
                String id = jsonObject.get("id").toString();
                jsonObject.remove("callback");
                jsonObject.remove("id");
                ByteArrayEntity byteArrayEntity = new ByteArrayEntity(jsonObject.toString().getBytes(), ContentType.create("application/json", "UTF-8"));
                HttpPost httpspost = new HttpPost("https://pvideo-api.65lzg.com/api/v1/classify");
                httpspost.setEntity(byteArrayEntity);
                CloseableHttpClient httpClient = HttpClients.createDefault();
                HttpResponse response = httpClient.execute(httpspost);
                String result = InputStreamToString(response.getEntity().getContent());
                JSONObject jsonResult = new JSONObject(result);
                JSONObject output = new JSONObject(jsonResult.get("result").toString());
                output.put("id",id);
                try {
                    httpspost = new HttpPost(callback);
                    byteArrayEntity = new ByteArrayEntity(output.toString().getBytes(), ContentType.create("application/json", "UTF-8"));
                    httpspost.setEntity(byteArrayEntity);
                    httpClient.execute(httpspost);
                }catch (Exception e){

                }
                httpClient.close();
            }

        }
    }
    public static String InputStreamToString(InputStream inputStream) throws IOException {
        byte[] bytes;
        bytes = new byte[inputStream.available()];
        inputStream.read(bytes);
        String str = new String(bytes);
        return  str;
    }
}
