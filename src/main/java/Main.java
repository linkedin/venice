import config.GlobalConfiguration;
import kafka.KafkaConsumer;
import kafka.KafkaProducer;
import message.Message;
import message.OperationType;
import storage.Store;

import java.util.Scanner;

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

        KafkaProducer kp = new KafkaProducer();
        Message msg;

        KafkaConsumer consumer = new KafkaConsumer("localhost:2181", "groupA", "kafka_key");
        consumer.run(1);

        // mocked input test
        Scanner reader = new Scanner(System.in);

        while (true) {
            System.out.println("Test Venice: ");
            String input = reader.nextLine();

            String[] commandArgs = input.split(" ");

            if (commandArgs[0].equals("put")) {

                msg = new Message(OperationType.PUT, commandArgs[1] + " " + commandArgs[2]);
                kp.sendMessage(msg);

                //storage.put(commandArgs[1], commandArgs[2]);
                System.out.println("Run a put: " + commandArgs[1] + " " + commandArgs[2]);
            }

            if (commandArgs[0].equals("get")) {
                System.out.println("Run a get: " + commandArgs[1]);
                System.out.println(storage.get(commandArgs[1]));
            }

        }

    }
}
