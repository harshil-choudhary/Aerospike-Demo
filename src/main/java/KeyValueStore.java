import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.WritePolicy;

public class KeyValueStore {

    public static void main(String[] args) {

        AerospikeClient client = new AerospikeClient("127.0.0.1", 3000);
        WritePolicy policy = new WritePolicy();

        Key key = new Key("test", "myset", "mykey");
        Bin bin = new Bin("mybin", "myvalue");
        client.put (policy, key, bin);

        Bin bin1 = new Bin("name", "John");
        Bin bin2 = new Bin("age", 25);
        client.put(policy, key, bin1, bin2);

        WritePolicy writePolicy = new WritePolicy();

        client.put(writePolicy, key, bin);
        Record record = client.get(writePolicy, key);

        Record record1 = client.get(policy, key, "name", "age");

        System.out.println(record);
        System.out.println(record1);

        //client.close();

    }
}
