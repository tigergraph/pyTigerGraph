# Mit Slave Scripts

This directory contains all scripts used for MIT management

## Scripts:

#### install_missing.sh:
**Purpose:** To detect missing software on vm and automatically install it if -i is used  
**Usage:**  
```bash
./install_missing.sh [-i]
-i: automatically install missing software
```

#### mach_user_management.sh:  
**Purpose:** Used to add/remove users from any machine  
**Usage:**  
```bash
./mach_user_management.sh [-h] <-a add/remove> <-u user_name> [-p password] [-s]
-h displays this message
-a specifies the action to take. It must be add or remove
-u specifies the user_name for the user to add/remove
-p specifies the password for the user (If not specified, no password will be used)
-i specifies the absolute paths of the public key(s) to be added to the user (If not specified, no keys will be added.
  Note: Key must already exist in destination machine.)
-s specifies that the user should have sudo (default: normal user)
```

#### mit_management:  
**Purpose:** Used to add/remove users/nodes on the MIT server  
**Usage:**  
```bash
./mit_management.sh [-h] <-a add_user/remove_user/add_node/remove_node> <parameters>
-h displays this message
-a specifies the action to take. It must be add_user, remove_user, add_node or remove_node

For Add User:
Usage ./mit_management.sh -a add_user <-u user_name> <-e email> <-j jira_username> <-z zulip_username> <-w work_location>
-u specifies the user_name for the user to add
-e specifies the email for the user to add
-j specifies the jira username for the user to add
-z specifies the zulip username for the user to add
-w specifies the work location for the user to add

For Remove User:
Usage ./mit_management.sh -a remove_user <-u user_name>
-u specifies the user_name for the user to remove

For Add Node:
Usage ./mit_management.sh -a add_node <-n node_name> <-s status> <-o offline_message> <-i node_ip>
-n specifies the node_name for the node to add
-s specifies the default status of the node
-o specifies the default offline message of the node
-i specifies the ip address of the node

For Remove Node:
Usage ./mit_management.sh -a remove_node <-n node_name>
-n specifies the node_name for the node to remove
```

#### periodic_check:  
**Purpose:** Used to periodically check running processes and internet access on VMs  
**Usage:**  
```bash
bash periodic_check &
```

#### run_on_slaves:  
**Purpose:** Used to copy files, run scripts and execute commands on machines with IPs specified in the config file. (Default config file is **IPs.conf**)  
**Usage:**  
```bash
./runOnSlaves.sh [-h] [-c file_to_copy] [-d directory_to_copy_to] [-f shell_script_to_execute] [-x command_to_execute] [-u user_to_use] [-i ssh_key_to_use]
-h  displays this message
-c  copies the specified file(s) to all IPs in the config file
-d  specifies the directory to copy the file to or to execute the script in (if not specified, the user's home directory ("~") will be used)
-f  executes the specified shell script(s) on all IPs in the config file
-x  executes the specified command on all IPs in the config file
-u  specifies the user to use (if not specified, user graphsql will be used)
-i  specifies the path of the ssh key to use (if not specified /home/graphsql/.ssh/id_rsa will be used) (WARNING: key MUST be setup correctly on the remote servers)
-p  specifies the file containing the parameters used for each machine when running the script specified with the -f flag.
The file must be in json format with each row in the form <IP of maching>:{<script_name>:<parameters>} or all:{<script_name>:<parameters>} if parameters for all machines are the same (if not specified no parameters are used) (if all is not specified an a ip is not found. No parameters are used for that machine.) (if a script name is not found in the file, no parameters are used for that script.)
-o  specifies the config file containing the IPs of the machines to run on. (if not specified IPs.conf will be used)
-P  specifies the password to use (if not specified, the key specified by -i will be used. If neither -P or -i is specified /home/graphsql/.ssh/id_rsa will be used.
Note the password will always be tried first followed by the key in all other cases.
note: if multiple options -c, -f or -x are selected the order will be copy file(-c), execute script(-f), and then execute command(-x)
```

#### updateConf.py:  
**Purpose:** Python script used to update the default config file **IPs.conf** for the **runOnSlaves.sh** script  
**Usage:**  
```bash
python updateConf.py [-h] [-r REMOVE [REMOVE ...]] [-a ADD [ADD ...]] [-b]

get the arguments

optional arguments:
  -h, --help            show this help message and exit
  -r REMOVE [REMOVE ...], --remove REMOVE [REMOVE ...]
                        specify removing the machine with the given ip
  -a ADD [ADD ...], --add ADD [ADD ...]
                        specify adding the machine with the given ip
  -b, --backwards       specify adding before removing instead of the default
                        removing before adding
```
