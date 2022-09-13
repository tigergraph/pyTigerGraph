import java.lang.Runtime;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Future;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;
import java.io.FileReader;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.opennebula.client.Client;
import org.opennebula.client.OneResponse;
import org.opennebula.client.ClientConfigurationException;
import org.opennebula.client.vm.VirtualMachine;
import org.opennebula.client.vm.VirtualMachinePool;
import org.opennebula.client.host.Host;
import org.opennebula.client.image.Image;


public class createVMInstances {
  public static void main(String args[]) {
    if (args.length == 0) {
      System.out.println("No arguments received, need OS parameter");
      System.exit(1);
    }
    System.out.println("Arguments received: " + args[0]);
    Client oneClient;
    VirtualMachinePool vmPool;
    String secret = "oneadmin:55582fdc770450ff91ef14d88979e3c2cdd6c4e2";
    String endpoint = "http://192.168.11.191:2633/RPC2";
    int timeout = 10 * 60; // seconds
    try {
      oneClient = new Client(secret, endpoint);
      /*
      ArrayList<String> VMList = createInstances(args[0], timeout, oneClient);
      String vmList = "";
      for (String vm : VMList) {
        // vm is "OS:vm_lable"
        vmList += vm + ";";
      }
      System.out.println("Created VM Instances: " + vmList);
      */
      if (args[0].equals("centos6")) {
        System.out.println("Created one VM Instance, vmName: none");
        return; 
      }
      String vmName = createOneInstance(args[0], oneClient);
      System.out.println("Created one VM Instance, vmName: " + vmName);
    }
    catch (ClientConfigurationException e) {
      System.out.println("The given Username:Password or endpoint is invalid.");
    }
    catch (Exception e) {
      System.out.println(e.getMessage());
    }
  }

  public static String createOneInstance(String OS, Client oneClient) {
    // create new VM
    JSONParser parser = new JSONParser();
    String jsonFile = "templates/disk_image_2_id.json";
    String template = "";
    try {
      JSONObject jsonObject = (JSONObject)parser.parse(new FileReader(jsonFile));
      System.out.println(jsonObject.get("diskImage2Id"));
      List<String> availableImages = Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8");
      Random rand = new Random();
      for (int i = 0; i < 8; i++) {
        int num = rand.nextInt(availableImages.size());
        String imageID = (String) ((JSONObject)jsonObject.get("diskImage2Id")).get(OS + "_disk_image_" + availableImages.get(num));
        Image image = new Image(Integer.parseInt(imageID), oneClient);
        OneResponse rc = image.info();
        if (!image.stateString().equalsIgnoreCase("READY")) {
          availableImages.remove(num);
        } else {
          template = Utils.readTemp("templates/" + OS + "_disk_image_" + availableImages.get(num));
          break;
        }
      }
    } catch (Exception e) {
      System.out.println("Failed to parse json file: " + jsonFile);
      return "none";
    }
    if (template.equals("")) {
      System.out.println("All disk images are occupied!");
      return "none";
    }
    System.out.print("Trying to allocate the virtual machine... ");
    OneResponse rc = VirtualMachine.allocate(oneClient, template);
    if (rc.isError()) {
      System.out.println("Failed to allocate virtual machine!");
      return "none";
    } 
    int vmNumber = Integer.parseInt(rc.getMessage());
    VirtualMachine vm = new VirtualMachine(vmNumber, oneClient);
    String ip = Utils.getIp(vm);
    System.out.println("Virtual Machine allocated successfully."
      +"The virtual machine number is " + vmNumber + " and the IP is "+ ip);
    return "slave_" + String.valueOf(vmNumber) + "_" + ip;
  }

  public ArrayList<String> createInstances(ArrayList<String> OSList, int timeout, Client oneClient)
        throws InterruptedException, ExecutionException {
 
    int threads = OSList.size();
    ExecutorService executor = Executors.newFixedThreadPool(threads);
  
    List<Future<String>> futures = new ArrayList<Future<String>>();
    for (final String os : OSList) {
      Callable<String> callable = new Callable<String>() {
        public String call() throws Exception {
          String ip = createOneInstance(os, oneClient);
          Runtime.getRuntime().exec("python addSlave.py " + ip);
          return "OS:VMLable";
        }
      };
      futures.add(executor.submit(callable));
    }
    executor.shutdown(); // Disable new tasks from being submitted

    boolean done = false;
    int count = 0;
    while (! done) {
      done = true;
      for (Future<String> future : futures) {
        done &= future.isDone();
      }
      TimeUnit.SECONDS.sleep(15);
      count += 15;
      if (! done && count >= timeout) {
        executor.shutdownNow(); // Cancel currently executing tasks
        done = true;
        System.out.println("Timeout: " + String.valueOf(timeout) + ", canceled creating VM tasks");
      }   
    }
    ArrayList<String> VMList = new ArrayList<String>();
    for (Future<String> future : futures) {
      if (future.isDone()) {
        VMList.add(future.get());
      }
    }
    return VMList;
  }
}
