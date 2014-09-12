import config.GlobalConfiguration;
import storage.Store;

public class Main {

    public static void main(String[] args) {

        System.out.println("Hello Venice!");

        // Things we will need to do in workflow:
        /*
            - Set up config
            - Listen for input

            - Get input
            - Get key from input
            - Map key to partition

         */

        // get configuration
        GlobalConfiguration cfg = new GlobalConfiguration();

        // initialize storage system with partition id at 1
        Store storage = new Store();
        storage.addPartition(1);

        storage.put("key_001", "value_001");
        storage.put("key_001", "value_002");

        System.out.println(storage.get("key_001"));

    }
}
