/**
 * This class with initiates a test server on opennebula when MIT or WIP is called
 * author: Kaiyuan (Kevin) Liu
 */
import org.opennebula.client.Client;
import org.opennebula.client.OneResponse;
import org.opennebula.client.ClientConfigurationException;
import org.opennebula.client.vm.VirtualMachine;
import org.opennebula.client.vm.VirtualMachinePool;
import org.opennebula.client.host.Host;
import java.lang.Runtime;
import java.util.Arrays;
import java.util.List;

public class Initiate_server {
  public static void main(String args[]) {
    Client oneClient;
    VirtualMachinePool vmPool;
    String secret = "oneadmin:55582fdc770450ff91ef14d88979e3c2cdd6c4e2";
    String endpoint = "http://192.168.11.191:2633/RPC2";
    String ip = "-1";
    try {
      oneClient = new Client(secret, endpoint);
      // get the IP of new created VM
      VirtualMachine vm = new VirtualMachine(26, oneClient);
      System.out.println(Utils.getIp(vm));
      
      vmPool = new VirtualMachinePool(oneClient);
      OneResponse rc = vmPool.info();

      if(rc.isError()) {
        System.out.println("Failed to get Virtual Machine List."
            + "Please make sure your login information is correct");
        throw new Exception( rc.getErrorMessage() );
      }

      printVMachinePool(vmPool);
      
      String[] hostList = {"5", "7", "13"};
      int vmCapacity = 18;
      int vmCount = getVMachineCount(vmPool, hostList);
      int vmAvailable = vmCapacity - vmCount;
      int instanceNumber = 4;
      if (vmAvailable >= 8) {
        instanceNumber = 8;
      }
      
      /*
      // create new VM
      rc = allocateTestVM(oneClient);
      
      if (rc.isError()) {
        System.out.println("Failed to allocate virtual machine!");
        throw new Exception(rc.getErrorMessage());
      }
      else {
        int vmNumber = Integer.parseInt(rc.getMessage());
        VirtualMachine vm = new VirtualMachine(vmNumber, oneClient);
        ip = Utils.getIp(vm);
        System.out.println("Virtual Machine allocated successfully."
          +"The virtual machine number is " + vmNumber + " and the IP is "+ ip);
        //Runtime.getRuntime().exec("python addSlave.py " + ip);
      }
      */
    }
    catch (ClientConfigurationException e) {
      System.out.println("The given Username:Password or endpoint is invalid.");
    }
    catch (Exception e) {
      System.out.println(e.getMessage());
    }
  }

  public static int getVMachineCount (VirtualMachinePool vmPool, String[] hostList) {
    int vmCount = 0;
    for (VirtualMachine vm : vmPool) {
      String hostID = Utils.getHostID(vm);
      if (Arrays.asList(hostList).contains(hostID)) {
        vmCount += 1;
      }
    }
    System.out.printf("Total number of VMs created: %d%n", vmCount);
    return vmCount;
  }

  /**
   * This method prints out the id, name and status of 
   * all virtual machines in the given virtual machine pool.
   * vmPool The virtual machine pool to print the info for.
   */
  public static void printVMachinePool (VirtualMachinePool vmPool) {
    String leftAlignFormat = "| %-8s | %-16s | %-8s | %-24s | %-8s |%n";
    String dash10 = new String(new char[10]).replace('\0', '-');
    String dash18 = new String(new char[18]).replace('\0', '-');
    String dash26 = new String(new char[26]).replace('\0', '-');
    String rowSparator = "+" + dash10 + "+" + dash18 + "+" + dash10 + "+" + dash26 + "+" + dash10 + "+%n";
    System.out.println("Number of VMs: " + vmPool.getLength());
    System.out.format(rowSparator);
    System.out.printf(leftAlignFormat, "User ID", "Name", "Status", "Host Name", "Host ID");
    System.out.format(rowSparator);
    for (VirtualMachine vm : vmPool) {
      String id   = vm.getId();
      String name = vm.getName();
      String status = vm.status();
      String hostName = Utils.getHostName(vm);
      String hostID = Utils.getHostID(vm);
      System.out.printf(leftAlignFormat, id, name, status, hostName, hostID);
    }
    System.out.format(rowSparator);
  }

  /**
   * This method allocates a virtural using a given client.
   * oneClient The client to use when allocating the virtual machine.
   */
  public static OneResponse allocateTestVM (Client oneClient) {
    String template = Utils.readTemp("templates/diskmaker_temp"); 

    System.out.print("Trying to allocate the virtual machine... ");
    OneResponse rc = VirtualMachine.allocate(oneClient, template);
    
    return rc;
  }
}
