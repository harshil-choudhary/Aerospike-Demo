import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class ReadingRecords {

    public static void main(String[] args) {
        int lowKeyVal = 0;
        int numKeys = 100000;
        int numReads = 100000;

        if (args.length > 0 && args.length != 3) {
            System.err.println("Invalid number of arguments");
            System.exit(1);
        } else if (args.length == 3)
            try {
                lowKeyVal = Integer.parseInt(args[0]);
                numKeys = Integer.parseInt(args[1]);
                numReads = Integer.parseInt(args[2]);
            } catch (NumberFormatException e) {
                System.err.println("All arguments must be integers");
                System.exit(1);
            }

        AerospikeClient client = new AerospikeClient("127.0.0.1", 3000);

        int randomInt;
        Key key = null;
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < numReads; i++) {
            randomInt = ThreadLocalRandom.current().nextInt(lowKeyVal, lowKeyVal + numKeys);
            key = new Key("test", "demo", randomInt);
            Record record = client.get(new Policy(), key);
            System.out.println("int:" + record.getInt("intbin") + " string: " + record.getString("strbin"));

        }

        long endTime = System.currentTimeMillis();
        System.out.println("records read/sec: " + numReads/((endTime - startTime)/1000));
        client.close();

    }
}
