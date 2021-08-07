import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class WritingRecords {

    public static void main(String[] args) {

        int lowKeyVal = 0;
        int numKeys = 100000;

        if (args.length>2 || args.length == 1) {
            System.err.println("Invalid number of arguments");
            System.exit(1);
        } else if (args.length == 2){
            try {
                lowKeyVal = Integer.parseInt(args[0]);
                numKeys = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                System.err.println("Both arguments must be integers");
                System.exit(1);
            }
        }

        AerospikeClient client = new AerospikeClient("127.0.0.1", 3000);

        int randomInt;
        Key key = null;

        long startTime = System.currentTimeMillis();

        for (int i = lowKeyVal; i < lowKeyVal + numKeys; i++) {
            key = new Key("test", "demo", i);
            randomInt = ThreadLocalRandom.current().nextInt(lowKeyVal, lowKeyVal + numKeys);
            Bin int_bin = new Bin("intbin", randomInt);
            Bin str_bin = new Bin("strbin", String.valueOf(randomInt));
            client.put(new WritePolicy(), key, int_bin, str_bin);
            System.out.println(key + " " + int_bin + " " + str_bin);
        }

        long endTime = System.currentTimeMillis();
        System.out.println("records written/sec: " + numKeys/((endTime - startTime)/1000));
        client.close();

    }
}
