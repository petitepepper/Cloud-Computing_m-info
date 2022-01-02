import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.InvalidObjectStateException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;


public class Woker {
	private static S3Client s3;

    public static void main(String[] args) throws IOException, InterruptedException {
        
        String queueInName = "Inbox_ye_wenjing_lab3_2021"; 
        String queueOutName = "Outbox_ye_wenjing_lab3_2021"; 
 
        Region region = Region.EU_WEST_3;
        s3 = S3Client.builder()
                .region(region)
                .build();

        SqsClient sqsClient = SqsClient.builder()
                .region(region)
                .build();
        

        // Create Queues 
        String queueInUrl = createQueue(sqsClient, queueInName);
        String queueOutUrl = createQueue(sqsClient, queueOutName);
        
        // Check message in Inbox queue every 1 min
        while(true) {
        	TimeUnit.MINUTES.sleep(1);
        	// Retrieve message
        	List<Message> messages = retrieveMessage(sqsClient,queueInUrl);
        	if (messages != null) {
        		
        		String[] names = messages.get(0).body().split(",");
        		String bucketName = names[0];
        		String filename = names[1];
        		
        		// Delete message
            	deleteMessages(sqsClient,queueInUrl,messages);
            	// TODO:Calculate(call a function)
            	
        		// TODO: write the result in a file
            	
            	// TODO: put the input and output file name in S3
            	
            	// TODO: send a message (names of input file and output file) in queue OUT 
            	
        	}
        	
        	
        	
        	
        }
        
    }

    
    
    
    // Create an queue in SQS
    public static String createQueue(SqsClient sqsClient,String queueName ) {

        try {
            System.out.println("[INFO]Create Queue: "+ queueName);
            CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();

            sqsClient.createQueue(createQueueRequest);

            GetQueueUrlResponse getQueueUrlResponse =
                sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
            String queueUrl = getQueueUrlResponse.queueUrl();
            System.out.println("[INFO]Get queue url: " + queueUrl);
            return queueUrl;
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return "";
  
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
    public static void getObject(S3Client s3, String bucketName, String key) throws NoSuchKeyException, InvalidObjectStateException, S3Exception, AwsServiceException, SdkClientException, IOException {
    	GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();
    	
    	// TODO: how to get an integer List?
    	s3.getObject(getObjectRequest);
    	System.out.println("[INFO]Retrieve file successfully");
    }
   
    // Create a bucket by using a S3Waiter object
    public static void createBucket(S3Client s3Client, String bucketName, Region region) {

        S3Waiter s3Waiter = s3Client.waiter();

        try {
            CreateBucketRequest bucketRequest = CreateBucketRequest.builder()
                    .bucket(bucketName)
                    .build();

            s3Client.createBucket(bucketRequest);
            HeadBucketRequest bucketRequestWait = HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build();

            // Wait until the bucket is created and print out the response
            WaiterResponse<HeadBucketResponse> waiterResponse = s3Waiter.waitUntilBucketExists(bucketRequestWait);
            waiterResponse.matched().response().ifPresent(System.out::println);
            System.out.println("[INFO]" + bucketName +" is ready");

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
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
    	sqsClient.sendMessage(SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(message)
                .delaySeconds(10)
                .build());
    	System.out.println("[INFO]Message send to queue successfully!");
    }
    
    private static ByteBuffer getRandomByteBuffer(int size) throws IOException {
        byte[] b = new byte[size];
        new Random().nextBytes(b);
        return ByteBuffer.wrap(b);
    }
}
