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

public class changeSnapshot {
  public static void main(String args[]) {
    if (args.length == 0) {
      System.out.println("No arguments received, need virtual machine name");
      System.exit(1);
    }
    System.out.println("Arguments received: " + args[0]);
    String os = args[0];
    Client oneClient;
    VirtualMachinePool vmPool;
    String secret = "oneadmin:55582fdc770450ff91ef14d88979e3c2cdd6c4e2";
    String endpoint = "http://192.168.11.191:2633/RPC2";
    int timeout = 900;
    try {
      oneClient = new Client(secret, endpoint);
      int vmID = getAvailableVM(os, oneClient);
      if (vmID == -1) {
        System.out.println("NO vm available");
        return;
      }
      String[] res = getVMInfo(vmID, os);
      int snapID = Integer.parseInt(res[0]);
      int diskID = Integer.parseInt(res[1]);
      String vmIP = res[2];
      if (snapID == -1 || diskID == -1 || vmIP.isEmpty()) {
        System.out.println("snapID: -1, or diskID: -1 is invalid; or vmIP is empty");
        return;
      }
      String result = switchSnapshot(vmID, vmIP, diskID, snapID, timeout, oneClient);
      System.out.println("Switched snapshot, vmName: " + result);
    }
    catch (ClientConfigurationException e) {
      System.out.println("The given Username:Password or endpoint is invalid.");
    }
    catch (Exception e) {
      System.out.println(e.getMessage());
    }
  }
 
  public static int getAvailableVM(String os, Client oneClient){
    VirtualMachinePool vmPool = new VirtualMachinePool(oneClient);
    OneResponse rc = vmPool.info();
    if(rc.isError()) {
      System.out.println("Failed to get Virtual Machine Pool.");
      return -1;
    }
    Random rand = new Random();
    for (VirtualMachine vm : vmPool) {
      if (!vm.status().equalsIgnoreCase("POFF")){
        System.out.println("The vmStatus: " + vm.status() + ", vmID: " + vm.getId());
      } else{
        String id   = vm.getId();
        String name = vm.getName();
        System.out.println("The vm is IN poweroff state, vmID: " + id);
        if (name.toLowerCase().contains(os.toLowerCase().substring(0, 6))) {
          System.out.println("The vm os is correct, vmName: " + name);
          return Integer.parseInt(id);
        } else{
          System.out.println("The vm os is NOT correct, vmName: " + name);
        }
      }
    }
    return -1;    
  }

  public static String[] getVMInfo(int vmID, String os) {
    JSONParser parser = new JSONParser();
    String jsonFile = "templates/vmID_2_snapshot_id.json";
    String snapID = "-1";
    String diskID = "-1";
    String vmIP = "";
    try {
      JSONObject jsonObject = (JSONObject)parser.parse(new FileReader(jsonFile));
      System.out.println(jsonObject.get(os));
      System.out.println(jsonObject.get("vmIP"));
      System.out.println(jsonObject.get("vmDiskID"));
      snapID = (String) ((JSONObject)jsonObject.get(os)).get(Integer.toString(vmID));
      System.out.println("snapID: " + snapID);
      vmIP = (String) ((JSONObject)jsonObject.get("vmIP")).get(Integer.toString(vmID));
      System.out.println(vmIP);
      diskID = (String) ((JSONObject)jsonObject.get("vmDiskID")).get(Integer.toString(vmID));
      System.out.println(diskID);
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
    return new String[]{snapID, diskID, vmIP};
  }

  public static String switchSnapshot(int vmID, String vmIP, int diskID, int snapID, int timeout, Client oneClient) {
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
    return "slave_" + String.valueOf(vmID) + "_" + vmIP;
  }
}
