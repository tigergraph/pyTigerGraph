import org.opennebula.client.Client;
import org.opennebula.client.OneResponse;
import org.opennebula.client.ClientConfigurationException;
import org.opennebula.client.vm.VirtualMachine;
import java.util.Arrays;
import java.util.List;

public class poweroffVMInstance {
  public static void main(String args[]) {
    if (args.length == 0) {
      System.out.println("No arguments received, need virtual machine name");
      System.exit(1);
    }
    System.out.println("Arguments received: " + args[0]);
    String[] vmName = args[0].split("_");
    System.out.println(Arrays.toString(vmName));
    int vmID = Integer.parseInt(vmName[1]);
    Client oneClient;
    String secret = "oneadmin:55582fdc770450ff91ef14d88979e3c2cdd6c4e2";
    String endpoint = "http://192.168.11.191:2633/RPC2";
    try {
      oneClient = new Client(secret, endpoint);
      String result = poweroffVM(vmID, oneClient);
      System.out.println("Poweroff VM result: " + result);
    }
    catch (ClientConfigurationException e) {
      System.out.println("The given Username:Password or endpoint is invalid.");
    }
    catch (Exception e) {
      System.out.println(e.getMessage());
    }
  }
 
  public static String poweroffVM(int vmID, Client oneClient) {
    // poweroff a VM
    System.out.print("Trying to poweroff the virtual machine... ");
    VirtualMachine vm = new VirtualMachine(vmID, oneClient);
    OneResponse rc = vm.poweroff(true);
    if (rc.isError()) {
      System.out.println("Failed to poweroff virtual machine: " + vmID);
      System.out.println("Error Message: " + rc.getErrorMessage());
      return "failed";
    } 
    System.out.println("Poweroff Virtual Machine successfully.\n"
      + "The virtual machine number is " + vmID);
    return "succeed";
  }
}
