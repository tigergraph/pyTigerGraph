import org.opennebula.client.Client;
import org.opennebula.client.OneResponse;
import org.opennebula.client.ClientConfigurationException;
import org.opennebula.client.vm.VirtualMachine;
import org.opennebula.client.vm.VirtualMachinePool;
import org.opennebula.client.host.Host;
import java.lang.Runtime;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.io.FileReader;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class revert {
  public static void main(String args[]) {
    if (args.length == 0) {
      System.out.println("No arguments received, need virtual machine name");
      System.exit(1);
    }
    System.out.println("Arguments received: " + args[0]);
    String vmIP = args[0];
    String imageVersion = args[1];
    Client oneClient;
    VirtualMachinePool vmPool;
    String secret = "oneadmin:55582fdc770450ff91ef14d88979e3c2cdd6c4e2";
    String endpoint = "http://192.168.11.191:2633/RPC2";
    int timeout = 900;
    try {
      oneClient = new Client(secret, endpoint);
      String[] vm_info = getAvailableVM(vmIP, oneClient, timeout);
      int vmID = Integer.parseInt(vm_info[0]);
      String vmName = vm_info[1];
      if (vmID == -1) {
        System.out.println("NO vm available");
        return;
      }
      String[] res = getVMInfo(vmIP, imageVersion);
      int snapID = Integer.parseInt(res[0]);
      int diskID = Integer.parseInt(res[1]);
      if (snapID == -1 || diskID == -1 || vmIP.isEmpty()) {
        System.out.println("snapID: -1, or diskID: -1 is invalid; or vmIP is empty");
        return;
      }
      String result = switchSnapshot(vmID, vmName, vmIP, diskID, snapID, timeout, oneClient);
      System.out.println("Switched snapshot, vmName: " + result);
    }
    catch (ClientConfigurationException e) {
      System.out.println("The given Username:Password or endpoint is invalid.");
    }
    catch (Exception e) {
      System.out.println(e.getMessage());
    }
  }


  public static String[] getAvailableVM(String vmIP, Client oneClient, int timeout){
    VirtualMachinePool vmPool = new VirtualMachinePool(oneClient);
    OneResponse rc = vmPool.info();
    if(rc.isError()) {
      System.out.println("Failed to get Virtual Machine Pool.");
      return new String[]{"-1", ""};
    }
    for (VirtualMachine vm : vmPool) {
      String id = vm.getId();
      String ip = Utils.getIp(vm);
      String name = vm.getName();
      System.out.println("The vmID: " + id);
      if (ip.equals(vmIP)) {
        System.out.println("The vm " + id + " is found, ip is " + ip);
        if (!vm.status().equalsIgnoreCase("POFF")) {
          System.out.print("Trying to poweroff the virtual machine... ");
          rc = vm.poweroff(true);
          if (rc.isError()) {
            System.out.println("Failed to poweroff virtual machine: " + id);
            System.out.println("Error Message: " + rc.getErrorMessage());
            return new String[]{"-1", ""};
          }
          System.out.println("Poweroff Virtual Machine successfully.\n"
            + "The virtual machine number is " + id);
          int count = 0;
          while (!Utils.getState(vm).equals("8")) {
            System.out.println("vm status is " + Utils.getState(vm));
            System.out.println("vm lcm status is " + Utils.getLcmState(vm));
            try {
              TimeUnit.SECONDS.sleep(3);
            } catch (InterruptedException e) {
              System.out.println("Failed to sleep!");
              e.printStackTrace();
            }
            count += 3;
            if (count > timeout) {
              return new String[]{"-1", ""};
            }
          }
        }
        return new String[]{id, name};
      }
    }
    return new String[]{"-1", ""};
  }

  public static String[] getVMInfo(String vmIP, String imageVersion) {
    JSONParser parser = new JSONParser();
    String jsonFile = "templates/vmIP_2_snapshot_id.json";
    String snapID = "-1";
    String diskID = "-1";
    try {
      JSONObject jsonObject = (JSONObject)parser.parse(new FileReader(jsonFile));
      System.out.println(jsonObject.get(vmIP));
      if (imageVersion.equalsIgnoreCase("TigerGraph")) {
        snapID = (String) ((JSONObject)jsonObject.get(vmIP)).get("SnapID");
      } else {
        snapID = (String) ((JSONObject)jsonObject.get(vmIP)).get("SnapID_2");
      }
      System.out.println("snapID: " + snapID);
      diskID = (String) ((JSONObject)jsonObject.get(vmIP)).get("DiskID");
      System.out.println(diskID);
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
    return new String[]{snapID, diskID};
  }

  public static String switchSnapshot(int vmID, String vmName, String vmIP, int diskID, int snapID, int timeout, Client oneClient) {
    // stop a VM
    System.out.println("Trying to revert the snapshot of the virtual machine... ");
    VirtualMachine vm = new VirtualMachine(vmID, oneClient);
    /** OneResponse rc = vm.poweroff(true);
    if (rc.isError()) {
      System.out.println("Failed to poweroff virtual machine: " + snapID);
      System.out.println("Error Message: " + rc.getErrorMessage());
      return "failed";
    } */
    OneResponse rc = vm.diskSnapshotRevert(diskID, snapID);
   // OneResponse rc = vm.snapshotRevert(snapID);
    if (rc.isError()) {
      System.out.println("Failed to revert snapshot of virtual machine: " + snapID);
      System.out.println("Error Message: " + rc.getErrorMessage());
      return "failed";
    }
    boolean resumed = false;
    int count = 0;
    while (!resumed) {
      try {
        TimeUnit.SECONDS.sleep(60);
      } catch (InterruptedException e) {
        System.out.println("Failed to sleep!");
        e.printStackTrace();
      }
      rc = vm.resume();
      if (rc.isError()) {
        System.out.println("Failed to resume of virtual machine: " + vmID);
        System.out.println("Error Message: " + rc.getErrorMessage());
      } else {
        resumed = true;
      }
      count += 60;
      if (count > timeout) {
        return "failed";
      }
    }
    System.out.println("Virtual Machine switch snapshot successfully.\n"
      + "The virtual machine number is " + vmID);
    return "test_" + vmName.split(" ")[0] + '_' + vmIP;
  }
}
