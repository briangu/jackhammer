import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.message.Message;
import kafka.producer.ProducerConfig;
import org.apache.commons.codec.binary.Hex;
import org.json.JSONException;
import org.json.JSONObject;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;


/**
 * Created by IntelliJ IDEA. User: bguarrac Date: 5/20/11 Time: 10:31 AM To change this template use File | Settings |
 * File Templates.
 */
public class Loader
{
  public static void main(String[] args)
  {
    String bootstrapUrl = "tcp://localhost:6666";
    StoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrl));
    StoreClient client = factory.getStoreClient("tweets");

    Properties props = new Properties();
    props.put("zk.connect", "127.0.0.1:2181");
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    ProducerConfig config = new ProducerConfig(props);
    Producer<String, String> producer = new Producer<String, String>(config);

    processData(
      "/Users/bguarrac/network-indexter/sensei.json",
      "/Users/bguarrac/network-indexter/meta.json",
      client,
      producer,
      "tweets");
  }

  public static void processData(String senseiFile, String metaFile, StoreClient voldemort, Producer<String,String> kafkaProducer, String kafkaTopic)
  {
    long id = 0;

    BufferedReader senseiReader = null;
    BufferedReader metaReader = null;
    try
    {
      senseiReader = new BufferedReader(new FileReader(senseiFile));
      metaReader = new BufferedReader(new FileReader(metaFile));

      String senseiLine;
      String metaLine;

      while((senseiLine = senseiReader.readLine()) != null)
      {
        metaLine = metaReader.readLine();

        JSONObject senseiObject = new JSONObject(senseiLine);
        JSONObject metaObject = new JSONObject(metaLine);

        // compute hash
        String sourceFile = senseiObject.get("sourceFile").toString();
        String fileData = readFileAsString(sourceFile);
        String hashId = Hex.encodeHexString(SHA256(sourceFile));

        if (!metaObject.get("sourceFile").equals(sourceFile))
        {
          throw new IllegalArgumentException("out of alignment!");
        }

        senseiObject.put("id", Long.toString(id++));
        senseiObject.put("hashId", hashId);
        senseiObject.put("time", System.currentTimeMillis());

        String senseiData = senseiObject.toString();
        System.out.println(senseiData);

        voldemort.put(hashId + "_META", metaObject.toString());
        voldemort.put(hashId + "_SENSEI", senseiData);
        voldemort.put(hashId, fileData);

        List<String> dataList = new ArrayList<String>();
        dataList.add(senseiData);
        ProducerData<String, String> kdata = new ProducerData<String, String>(kafkaTopic, dataList);
        kafkaProducer.send(kdata);
      }
    }
    catch (FileNotFoundException e)
    {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    catch (IOException e)
    {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    catch (JSONException e)
    {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    catch (Exception e)
    {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    finally
    {
      if (senseiReader != null)
      {
        try
        {
          senseiReader.close();
        }
        catch (IOException e)
        {
          e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
      }
      if (metaReader != null)
      {
        try
        {
          metaReader.close();
        }
        catch (IOException e)
        {
          e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
      }
    }
  }

  private static String readFileAsString(String filePath) throws java.io.IOException{
    byte[] buffer = new byte[(int) new File(filePath).length()];
    BufferedInputStream f = new BufferedInputStream(new FileInputStream(filePath));
    f.read(buffer);
    return new String(buffer);
  }

  public static byte[] SHA256(String filename) throws Exception
  {
    MessageDigest md = MessageDigest.getInstance("SHA-256");
    FileInputStream fis = new FileInputStream(filename);

    byte[] dataBytes = new byte[16384];

    int nread;
    while ((nread = fis.read(dataBytes)) != -1) {
      md.update(dataBytes, 0, nread);
    }
    byte[] mdbytes = md.digest();

    return mdbytes;
  }
}
