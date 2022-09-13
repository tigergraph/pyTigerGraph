import org.opennebula.client.Client;
import org.opennebula.client.OneResponse;
import org.opennebula.client.ClientConfigurationException;
import org.opennebula.client.vm.VirtualMachine;
import org.opennebula.client.vm.VirtualMachinePool;
import org.opennebula.client.host.Host;
import java.lang.Runtime;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutorService;

public class deleteVMInstance {
  public static void main(String args[]) {
    if (args.length == 0) {
      System.out.println("No arguments received, need virtual machine name");
      System.exit(1);
    }
    System.out.println("Arguments received: " + args[0]);
    String[] vmName = args[0].split("_");
    System.out.println(Arrays.toString(vmName));
    int vmID = Integer.parseInt(vmName[1]);
    int timeout = 600;
    Client oneClient;
    VirtualMachinePool vmPool;
    String secret = "oneadmin:55582fdc770450ff91ef14d88979e3c2cdd6c4e2";
    String endpoint = "http://192.168.11.191:2633/RPC2";
    try {
      oneClient = new Client(secret, endpoint);
      String result = deleteOneInstance(vmID, timeout, oneClient);
      System.out.println("Delete VM result: " + result);
    }
    catch (ClientConfigurationException e) {
      System.out.println("The given Username:Password or endpoint is invalid.");
    }
    catch (Exception e) {
      System.out.println(e.getMessage());
    }
  }

  public static String deleteOneInstance(int vmID, int timeout, Client oneClient) {
    // stop a VM
    System.out.print("Trying to stop the virtual machine... ");
    VirtualMachine vm = new VirtualMachine(vmID, oneClient);
    boolean terminated = false;
    int count = 0;
    while (!terminated) {
      OneResponse rc = vm.terminate(true);
      if (rc.isError()) {
        System.out.println("Failed to stop virtual machine!");
        System.out.println("Error Message: " + rc.getErrorMessage());
      } else {
        terminated = true;
      }
      try {
        TimeUnit.SECONDS.sleep(60);     
      } catch (InterruptedException e) {
        System.out.println("Failed to sleep!");
        e.printStackTrace();
      }
      count += 60;
      if (count > timeout) {
        return "failed";
      }
    }
    System.out.println("Virtual Machine stopped successfully.\n"
      + "The virtual machine number is " + vmID);
    return "succeed";
  }
}
