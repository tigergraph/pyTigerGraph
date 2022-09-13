import org.opennebula.client.Client;
import org.opennebula.client.OneResponse;
import org.opennebula.client.ClientConfigurationException;
import org.opennebula.client.vm.VirtualMachine;
import org.opennebula.client.vm.VirtualMachinePool;
import org.opennebula.client.host.Host;
import java.lang.Runtime;
import java.util.Arrays;
import java.util.List;

public class getInstanceNumber {
  public static void main(String args[]) {
    Client oneClient;
    VirtualMachinePool vmPool;
    String secret = "oneadmin:55582fdc770450ff91ef14d88979e3c2cdd6c4e2";
    String endpoint = "http://192.168.11.191:2633/RPC2";
    String ip = "-1";
    try {
      oneClient = new Client(secret, endpoint);
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
      System.out.printf("InstanceNumber: %d%n", instanceNumber);
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

  public static void printVMachinePool (VirtualMachinePool vmPool) {
    String leftAlignFormat = "| %-8s | %-24s | %-8s | %-24s | %-8s |%n";
    String dash10 = new String(new char[10]).replace('\0', '-');
    String dash18 = new String(new char[18]).replace('\0', '-');
    String dash26 = new String(new char[26]).replace('\0', '-');
    String rowSparator = "+" + dash10 + "+" + dash26 + "+" + dash10 + "+" + dash26 + "+" + dash10 + "+%n";
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
}
