package Test;

import java.io.Serializable;
import java.util.Map.Entry;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.SerializationUtils;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class Main {

    private static String stringSerialize(Serializable source) {
        byte[] data = SerializationUtils.serialize(source);
        return Base64.encodeBase64String(data);
    }
    private static Object stringDeSerialize(String string) {
        Object o = SerializationUtils.deserialize( Base64.decodeBase64(string) );
        return o;
    }
    public static class TestDataBox implements Serializable{
        public String name;
        public Integer number;
        public String toString(){
            return name + " " + number;
        }
    }
    public static void printMessage(){
        
    }
    public static void main(String[] args) {
        /*
         * The ProfileCredentialsProvider will return your [default] credential
         * profile by reading from the credentials file located at
         * (~/.aws/credentials).
         */
        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                "Cannot load the credentials from the credential profiles file. "
                    + "Please make sure that your credentials file is at the correct "
                    + "location (~/.aws/credentials), and is in valid format.",
                    e);
        }
        
        AmazonSQS sqs = new AmazonSQSClient(credentials);
        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
        sqs.setRegion(usWest2);
        
        System.out.println("===========================================");
        System.out.println("Getting Started with Amazon SQS");
        System.out.println("===========================================\n");
        
        Main.TestDataBox test1 = new Main.TestDataBox();
        test1.name = "Ayye";
        test1.number = 1;
        
        // Send a message.
        String calAmpQURL = "https://sqs.us-west-2.amazonaws.com/467537215423/test-sqs-q";
        String payload1 = stringSerialize(test1);
        System.out.println("Sending a message to MyQueue. Payload: " + payload1 + "\n");
        
        SendMessageRequest mreq = new SendMessageRequest();
        mreq = mreq.withQueueUrl(calAmpQURL);
        mreq = mreq.withMessageBody(payload1);
        sqs.sendMessage(mreq);
        
        // Receive messages.
        System.out.println("Receiving messages from MyQueue.\n");
        ReceiveMessageRequest receiveMessageRequest;
        receiveMessageRequest = new ReceiveMessageRequest(calAmpQURL);
        receiveMessageRequest = receiveMessageRequest.withMaxNumberOfMessages(1);
        receiveMessageRequest = receiveMessageRequest.withWaitTimeSeconds(1);
        
        Message message = sqs.receiveMessage(receiveMessageRequest).getMessages().get(0);
        
        System.out.println("  Message");
        System.out.println("    MessageId:     " + message.getMessageId());
        System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
        System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
        System.out.println("    Body:          " + message.getBody());
        for (Entry<String, String> entry : message.getAttributes().entrySet()) {
            System.out.println("  Attribute");
            System.out.println("    Name:  " + entry.getKey());
            System.out.println("    Value: " + entry.getValue());
        }
        try{
            System.out.println( ((TestDataBox) stringDeSerialize(message.getBody())) ); 
            System.out.println("Deleting a message.\n");
            DeleteMessageRequest dmr = new DeleteMessageRequest();
            dmr = dmr.withQueueUrl(calAmpQURL);
            dmr = dmr.withReceiptHandle(message.getReceiptHandle());
            sqs.deleteMessage( dmr );
            System.out.println("Deleted");
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }
}
