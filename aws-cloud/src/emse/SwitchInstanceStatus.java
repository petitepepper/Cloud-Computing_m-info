package emse;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.StartInstancesRequest;
import software.amazon.awssdk.services.ec2.model.StopInstancesRequest;


public class SwitchInstanceStatus {

  public static void main(String[] args) {

	Region region = Region.EU_WEST_3;
	
    String instanceId = args[0];
    String action = args[1];
    boolean start;
    
    Ec2Client ec2 = Ec2Client.builder()
            .region(region)
            .build();

    if(action.equals("start")) {
        start = true;
    } else {
        start = false;
    }

    if(start) {
        startInstance(ec2, instanceId);
    } else {
        stopInstance(ec2, instanceId);
    }
    ec2.close();
  }
  
  
  
  public static void startInstance(Ec2Client ec2, String instanceId) {

      StartInstancesRequest request = StartInstancesRequest.builder()
              .instanceIds(instanceId)
              .build();

      ec2.startInstances(request);
      System.out.printf("Successfully started instance %s", instanceId);
  }
  
  
  
  public static void stopInstance(Ec2Client ec2, String instanceId) {

      StopInstancesRequest request = StopInstancesRequest.builder()
              .instanceIds(instanceId)
              .build();

      ec2.stopInstances(request);
      System.out.printf("Successfully stopped instance %s", instanceId);
  }

  
  
}