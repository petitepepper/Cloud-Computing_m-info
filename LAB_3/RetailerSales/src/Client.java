
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.InvalidObjectStateException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;


public class Client {
	
	private static S3Client s3;

    public static void main(String[] args) throws IOException, InterruptedException {

        final String USAGE = "\n" +
                "Usage:\n" +
                "    <bucketName> <key> <inbox queuy url> <outbox queuy url> \n" ;
  

        if (args.length != 4) {
            System.out.println(USAGE);
            System.exit(1);
        }

 
        Region region = Region.EU_WEST_3;
        s3 = S3Client.builder()
                .region(region)
                .build();

        SqsClient sqsClient = SqsClient.builder()
                .region(region)
                .build();
        

        String bucketName = args[0];
        String key        = args[1]; //file(to upload) name 
        String queueInUrl = args[2];
        String queueOutUrl= args[3];
        
        /*
         * Note that this part below should be run only once!
         * 		otherwise you will need to change the name of file to upload MANUALLY!
         */
        
        ///////////////////////////
        // Upload file to S3 bucket
        UploadObject(s3,bucketName,key);
        
        // Send message to Inbox queue
        String message = bucketName + "," + key;
        sendMessage(sqsClient,queueInUrl,message);
        ///////////////////////////
        
        
        boolean waitResponse = true; 
        // Check message in outbox queue every 1 min
        while(waitResponse) {
        	// Retrieve message
        	List<Message> messages = retrieveMessage(sqsClient,queueOutUrl);
        	if (messages.size()!=0 && messages!=null) {
        		for(Message each:messages) {
	        		String[] fileNames = each.body().split(","); 
	        		String outputFileName = fileNames[1];
	        		File file = new File("src\\temp\\client\\"+outputFileName);	
	            	// Read file from S3 & save it
	            	getObject(s3,bucketName,outputFileName,file);
        		}
        		
        		// End waiting
            	waitResponse = false;
        		// Delete message
            	deleteMessages(sqsClient,queueOutUrl,messages);
        	}
        	
        	//Wait for a minute
        	TimeUnit.MINUTES.sleep(1);
        }
        
        
        
    }

	

    // Upload an object 
    private static void UploadObject(S3Client s3, String bucketName, String key) throws IOException {
    	PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        s3.putObject(objectRequest, RequestBody.fromByteBuffer(getRandomByteBuffer(10_000)));
        System.out.println("[INFO]" + key + " is uploaded to " + bucketName);
    }

    // Send a message to the queue
    public static void sendMessage(SqsClient sqsClient, String queueUrl, String message) {
    	Long messageId = System.currentTimeMillis();
    	sqsClient.sendMessage(SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(message)
                .messageGroupId(""+messageId)
                .messageDeduplicationId(""+messageId)
                .build());
    	System.out.println("[INFO]Message send to queue successfully!");
    }
   
    // Retrieve message
    public static List<Message> retrieveMessage(SqsClient sqsClient, String queueUrl) {
        try
    	{ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(1)
                .build();
            List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).messages();
            System.out.println("[INFO]Retrieve messages successfully");
            return messages;
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return null;
	}
    
    // Delete Message
    public static void deleteMessages(SqsClient sqsClient, String queueUrl,  List<Message> messages) {
        System.out.println("[INFO]Deleting messages");

        try {
            for (Message message : messages) {
                DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle())
                    .build();
                sqsClient.deleteMessage(deleteMessageRequest);
                System.out.println("[INFO]Delete messages successfully");
            }
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
   } 
   
    // Retrieve file from S3
    public static void getObject(S3Client s3, String bucketName, String key, File file) throws NoSuchKeyException, InvalidObjectStateException, S3Exception, AwsServiceException, SdkClientException, IOException {
    	GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();
    	
    	s3.getObject(getObjectRequest,ResponseTransformer.toFile(file));
    	System.out.println("[INFO]Retrieve file successfully");
    }
   
    private static ByteBuffer getRandomByteBuffer(int size) throws IOException {
        byte[] b = new byte[size];
        new Random().nextBytes(b);
        return ByteBuffer.wrap(b);
    }
}
