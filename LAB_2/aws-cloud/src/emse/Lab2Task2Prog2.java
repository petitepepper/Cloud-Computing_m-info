package emse;

import java.io.IOException;
import java.util.List;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.InvalidObjectStateException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

public class Lab2Task2Prog2 {

   

    public static void main(String[] args) throws IOException {

        final String USAGE = "\n" +
                "Usage:\n" +
                "    <bucketName> <key>\n\n" +
                "Where:\n" +
                "    bucketName - the Amazon S3 bucket to create.\n\n" +
                "    key - the key to use.\n\n" +
                "    queueUrl - the url of queue";
        

        if (args.length != 3) {
            System.out.println(USAGE);
            System.exit(1);
        }

        String bucketName = args[0];
        String key = args[1];        // the name of object to upload
        String queueUrl = args[2];
 
        Region region = Region.EU_WEST_3;
 
        S3Client s3 = S3Client.builder()
                .region(region)
                .build();

        SqsClient sqsClient = SqsClient.builder()
                .region(region)
                .build();
    
        
        // Retrieve the message from queue in SQS
        List<Message> messages = retrieveMessage(sqsClient,queueUrl);
        
        // Delete message
        deleteMessages(sqsClient,queueUrl,messages);
        
        // Retrieve file from S3
        getObject(s3, bucketName, key);
        
        // Delete file
        deleteObject(s3, bucketName, key);
        
        // Delete bucket
        deleteBucket(s3, bucketName);
        
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
    
  
    
    // Delete object
    public static void deleteObject(S3Client s3, String bucketName, String key) {
    	 DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                 .bucket(bucketName)
                 .key(key)
                 .build();

         s3.deleteObject(deleteObjectRequest);
         System.out.println("[INFO]Delete file successfully");
    }
    
    
    // Delete Bucket
    public static void deleteBucket(S3Client client, String bucket) {
        DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder()
                .bucket(bucket)
                .build();
        client.deleteBucket(deleteBucketRequest);
        System.out.println("[INFO]Delete bucket successfully");
        
     }
    
    

  
}