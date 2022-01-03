import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.InvalidObjectStateException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;


public class Worker {
	private static S3Client s3;

    public static void main(String[] args) throws IOException, InterruptedException {
        
    	Region region = Region.EU_WEST_3;
        s3 = S3Client.builder()
                .region(region)
                .build();

        SqsClient sqsClient = SqsClient.builder()
                .region(region)
                .build();
        
        String queueInName = "Inbox_ye_wenjing_lab3.fifo"; 
        String queueOutName = "Outbox_ye_wenjing_lab3.fifo"; 

        // Create Queues 
        String queueInUrl  = createQueue(sqsClient, queueInName);
        String queueOutUrl = createQueue(sqsClient, queueOutName);
        
        
        // Check message in Inbox queue every 1 min
        while(true) {
        	// Retrieve message
        	List<Message> messages = retrieveMessage(sqsClient,queueInUrl);
        	if (messages.size()!=0 && messages!=null) {
        		for(Message each:messages) {
        			String[] names = each.body().split(",");
        			String bucketName = names[0];
            		String inputFileName = names[1];
            		File file = new File("src\\temp\\worker\\"+ System.currentTimeMillis() + ".csv"); //Store the file gotten from s3 in local
            		String outputFileName = inputFileName.split(".csv")[0] + "_result.txt";
 	
                	// Get object from s3
                	getObject(s3,bucketName,inputFileName,file);
                	
                	// Calculate(call a function)
                	CalculateCSV(file,bucketName,outputFileName);
                	
                	// Put the output file in S3
                	UploadObject(s3,bucketName,outputFileName);
                	
                	// Send a message (names of input file and output file) in queue OUT 
                	sendMessage(sqsClient,queueOutUrl,inputFileName+","+outputFileName);
                }
        		
        		// Delete message
            	deleteMessages(sqsClient,queueInUrl,messages);
        	}
        	
        	System.out.println("[INFO]Waiting for a minute ...");
    		TimeUnit.MINUTES.sleep(1);
        	
        }
        
    }
    
    

    
    
    
    // Create an queue in SQS
    public static String createQueue(SqsClient sqsClient,String queueName ) {

        try {
        	
    		System.out.println("[INFO]Creating Queue: "+ queueName);
    		//Set attribute
    		Map<QueueAttributeName, String> queueAttributes = new HashMap<>();
            queueAttributes.put(QueueAttributeName.FIFO_QUEUE, "true");
            
            CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .attributes(queueAttributes)
                .build();

            sqsClient.createQueue(createQueueRequest);

            GetQueueUrlResponse getQueueUrlResponse =
                sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
            String queueUrl = getQueueUrlResponse.queueUrl();
            System.out.println("[INFO]Get queue url: " + queueUrl);
            return queueUrl;
        	
            
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            //System.exit(1);
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
    public static void getObject(S3Client s3, String bucketName, String key, File file) throws NoSuchKeyException, InvalidObjectStateException, S3Exception, AwsServiceException, SdkClientException, IOException {
    	GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();
    	
    	s3.getObject(getObjectRequest, ResponseTransformer.toFile(file));
    	System.out.println("[INFO]Retrieve file successfully");
    }
   
   
    // Calculate
    private static void CalculateCSV(File file,String bucketName,String outputFileName) throws UnsupportedEncodingException {
    	System.out.println("[INFO]Start calculating");
		// Define variables
    	Integer totalAmount = 0;
    	Integer totalSalesNumber = 0;
		HashMap<String,Integer> nProducts = new HashMap<String,Integer>();
		HashMap<String,Integer> nCountries = new HashMap<String,Integer>();
		HashMap<String,Integer> amountPerProduct = new HashMap<String,Integer>();
		HashMap<String,Integer> amountPerCountry = new HashMap<String,Integer>();
		
		try{
			Scanner scanner = new Scanner(file);
			// Read file
			while(scanner.hasNextLine()) {
				String str = scanner.nextLine();
				String[] strs = str.split(",");
			
				//Index: '2' for product name, '3' for amount, '7' for country
				totalAmount += Integer.parseInt(strs[3]);
				totalSalesNumber += 1;
				
				if(!nProducts.containsKey(strs[2])) {
					nProducts.put(strs[2], 1);
				}else nProducts.put(strs[2], nProducts.get(strs[2])+1);
				
				if(!nCountries.containsKey(strs[7])) {
					nCountries.put(strs[7], 1);
				}else nCountries.put(strs[7], nCountries.get(strs[7])+1);
				
				if(!amountPerCountry.containsKey(strs[7])) {
					amountPerCountry.put(strs[7], Integer.parseInt(strs[3]));
				}else amountPerCountry.put(strs[7], amountPerCountry.get(strs[7])+Integer.parseInt(strs[3]));
				
				if(!amountPerProduct.containsKey(strs[2])) {
					amountPerProduct.put(strs[2], Integer.parseInt(strs[3]));
				}else amountPerProduct.put(strs[2], amountPerProduct.get(strs[2])+Integer.parseInt(strs[3]));
				   					
			}
			scanner.close();
			
			// Output the result
			PrintWriter writer = new PrintWriter(outputFileName, "UTF-8");
			writer.println("Total Number of Sales: " + totalSalesNumber);
			writer.println("Total Amount Sold: " + totalAmount);
			
			// Calculate average
			for(Map.Entry<String, Integer> entry:amountPerProduct.entrySet()) {
				String  productName = entry.getKey();
				Integer totalAmountPerProduct = entry.getValue();
				Integer n_times = nProducts.get(productName);
				writer.println("Product : " + productName + ", \t Average price: " + totalAmountPerProduct/n_times);
			}
			
			for(Map.Entry<String, Integer> entry:amountPerCountry.entrySet()) {
				String  countryName = entry.getKey();
				Integer totalAmountPerCountry = entry.getValue();
				Integer n_times = nCountries.get(countryName);
				writer.println("Country : " + countryName + ", \t Average price: " + totalAmountPerCountry/n_times);
			}
			
			writer.close();	
		}catch(FileNotFoundException e) {
			e.printStackTrace();
		}
    	
		System.out.println("[INFO]Calculation finished: result is stored in "+outputFileName);
    			
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
    
    
    
    private static ByteBuffer getRandomByteBuffer(int size) throws IOException {
        byte[] b = new byte[size];
        new Random().nextBytes(b);
        return ByteBuffer.wrap(b);
    }
}
