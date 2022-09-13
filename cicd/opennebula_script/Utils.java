/*This class defines utilities used by the main class
 *Author: Kaiyuan Liu
 */
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.opennebula.client.vm.VirtualMachine;
import org.opennebula.client.Client;
public class Utils {
  /**
   *This method reads the vitural machine template in a file with the given file name.
   *templateFileName - the given file name
   */
  public static String readTemp(String templateFileName) {
    String template = "";
    try {
      String line;
      FileReader fileReader = new FileReader(templateFileName);
      BufferedReader bufferedReader = new BufferedReader(fileReader);

      while((line = bufferedReader.readLine()) != null) {
        template = template+"\n"+line;
      }

      bufferedReader.close();
    }
    catch(FileNotFoundException e) {
      System.out.println("Cannot open file '" + templateFileName + "'");
    }
    catch(IOException ex) {
      System.out.println("Cannot read file '" + templateFileName + "'");
    }
    return template;
  }

  /**
   *This class extracts the ip address of a given virtual machine in opennebula
   *vmNumber: the virtual machine number
   *vmClient: the client used to initiate the virtual machine
   */
  public static String getIp(VirtualMachine vm){
    String ip = "";
    ip=vm.info().getMessage();
    ip = vm.xpath("/VM/TEMPLATE/NIC/IP");
    return ip;
  }

  public static String getHostName(VirtualMachine vm){
    String hostName = "";
    vm.info().getMessage();
    hostName = vm.xpath("/VM/HISTORY_RECORDS/HISTORY/HOSTNAME");
    return hostName;
  }

  public static String getHostID(VirtualMachine vm){
    String hostID = "";
    vm.info().getMessage();
    hostID = vm.xpath("/VM/HISTORY_RECORDS/HISTORY/HID");
    return hostID;
  }

  public static String getState(VirtualMachine vm){
    String state = "";
    vm.info().getMessage();
    state = vm.xpath("/VM/STATE");
    return state;
  }

  public static String getLcmState(VirtualMachine vm){
    String state = "";
    vm.info().getMessage();
    state = vm.xpath("/VM/LCM_STATE");
    return state;
  }

}
