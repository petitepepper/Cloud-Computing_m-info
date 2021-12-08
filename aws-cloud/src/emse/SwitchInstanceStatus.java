package emse;

import java.util.List;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.Reservation;
import software.amazon.awssdk.services.ec2.model.StartInstancesRequest;
import software.amazon.awssdk.services.ec2.model.StopInstancesRequest;


public class SwitchInstanceStatus {

    public static void main(String[] args) {

        Region region = Region.EU_WEST_3;

        String instanceId = args[0];
//    String action = args[1];

        
        Ec2Client ec2 = Ec2Client.builder()
                .region(region)
                .build();

        DescribeInstancesRequest request = DescribeInstancesRequest.builder().instanceIds(instanceId).build();
        DescribeInstancesResponse response = ec2.describeInstances(request);

        List<Reservation> reservations = response.reservations();
        Instance instance = reservations.get(0).instances().get(0);
        String currentStatus = instance.state().nameAsString();
        System.out.println(currentStatus);

        if (currentStatus.equals("running")) {
        	System.out.println("stop");
        	stopInstance(ec2, instanceId);
        } else {
        	System.out.println("start");
        	startInstance(ec2, instanceId);
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