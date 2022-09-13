/*
* some common function and variable defind in this file
*/

// import groovy library
import groovy.time.*
import groovy.json.*
import net.sf.json.*

product_dir = "${env.HOME}/product"
slave_script_dir = "mit/slave_script"
jenkins_script_dir = "mit/jenkins_script"

config_dir = "${jenkins_script_dir}/config"
python_script_dir = "${jenkins_script_dir}/python_script"
python3_script_dir = "${jenkins_script_dir}/python3_script"
shell_script_dir = "${jenkins_script_dir}/shell_script"
unittest_folder = "${jenkins_script_dir}/unittest"
integration_folder = "${jenkins_script_dir}/integration_test"


// swtich jenkins config file as the jenkins id
def jenins_config_file = ( ! env.JENKINS_ID || env.JENKINS_ID =~ "prod_sv4" ) ? "${config_dir}/config.json" : "${config_dir}/config_${env.JENKINS_ID}.json"
CONFIG = readJSON file: jenins_config_file

//w/a to support different jenkins servers
//add jenkins_public_ip for cloud
echo "${JENKINS_URL}"
CONFIG['jenkins_public_ip'] = JENKINS_URL.toURL().host
CONFIG['jenkins_port'] = JENKINS_URL.toURL().port
env.JENKINS_ID = CONFIG['jenkins_id']

// log_dir is original all log folders
log_dir = CONFIG['log_dir']
timecost_config_folder = "${log_dir}/config/timecost_config"
test_config_folder = "${log_dir}/config/test_config"

TEST_CONFIG =

test_config_file =
repo_list_file =
match_job_dir = "none"

// gle auto build dir
ROOT_DIR = "${env.HOME}/auto_build_pipeline"

// Set github credentials for qa_tigergraph to use with MIT using credentials from jenkins
withCredentials([usernamePassword(credentialsId: 'qa-graphsql', usernameVariable: 'QA_USER', passwordVariable: 'QA_TOKEN')]) {
  env.MIT_GIT_USER = QA_USER
  env.MIT_GIT_TOKEN = QA_TOKEN
}

// Set github credentials for qa_tigergraph to use with MIT using credentials from jenkins
if (T_JOB_ID == 'WIP') {
  withCredentials([usernamePassword(credentialsId: 'qe-gworkspace', usernameVariable: 'GWORK_USER', passwordVariable: 'GWORK_TOKEN')]) {
    env.MIT_GW_USER = GWORK_USER
    env.MIT_GW_TOKEN = GWORK_TOKEN
  }
} else {
  withCredentials([usernamePassword(credentialsId: 'qa-gworkspace', usernameVariable: 'GWORK_USER', passwordVariable: 'GWORK_TOKEN')]) {
    env.MIT_GW_USER = GWORK_USER
    env.MIT_GW_TOKEN = GWORK_TOKEN
  }
}

// ftp server info
ftpSrv='ftp.graphsql.com'
ftpUrl="ftp://${ftpSrv}"
ftpPath="/product/hourly"

//Migrate default MIT machines to k8s cluster
if (MACHINE == "MIT" || MACHINE == "hourly" || MACHINE == "K8S") {
  MACHINE = "k8s"
} else if (MACHINE == "mit") {
  MACHINE = "MIT"
} else if (MACHINE == "HOURLY") {
  MACHINE = "hourly"
} else if (MACHINE == "gcc48") {
  MACHINE = "k8s_gcc48"
} else if (MACHINE == "gcc9") {
  MACHINE = "k8s_gcc9"
}
// test os mapping dict
// centos6/ubuntu16 is eol, remove from list and add centos8
/*if (MACHINE =~ "k8s") {
  if (BASE_BRANCH =~ "^tg_3[.]1[.]" || BASE_BRANCH =~ "^tg_2[.]") {
    test_os_dict = ['centos7', 'centos8', 'ubuntu16', 'ubuntu18']
  } else {
    test_os_dict = ['centos7', 'centos8', 'ubuntu18', 'ubuntu20']
  }
} else if (T_JOB_ID == 'HOURLY') {
  test_os_dict = ['centos6', 'centos7', 'ubuntu14', 'ubuntu16']
} else {
  test_os_dict = ['centos7', 'ubuntu16']
}
*/
special_ut_dict = ['vis', 'gap', 'gus', 'gst', 'gapE2E', 'gusE2E', 'gstE2E', 'cqrs']

// cluster and single server config
cluster_nodes_num = CONFIG['cluster_size'] as Integer

// if it is not build_job set value for default CLUSTER_TYPE and update cluster_nodes_num accordingly
if ( JOB_ID != "build_job" ) {
  if (CLUSTER_TYPE.matches("cluster-\\d+")){
    cluster_nodes_num = CLUSTER_TYPE.split("-")[1].toInteger()
    echo "Reset cluster nodes num to ${cluster_nodes_num} as parameter ${CLUSTER_TYPE}"
  }
  if (CLUSTER_TYPE == "single"){
    cluster_nodes_num = 1
  } else {
    CLUSTER_TYPE = "cluster"
  }
}

SPECIAL_APPROVED = false
REPO_APPROVED = false

/*
* init function for each pipeline to set variables and create log dir
*/
def init() {
  echo "Start init()"
  // get buildUser from environment
  wrap([$class: 'BuildUser']) {
    BUILD_USER = env.BUILD_USER
  }
  NO_FAIL = NO_FAIL.trim()
  NODE_IP = sh(script: "hostname -I | cut -d' ' -f1", returnStdout: true).trim()
  if (MACHINE =~ "k8s"){
    NODE_NAME = env.NODE_NAME+"_"+NODE_IP
  } else {
    NODE_NAME = env.NODE_NAME
  }
  env.PRODUCT = product_dir
  env.LANG = "en_US.UTF-8"
  env.LC_ALL = env.LANG

  // it is not test_job/build_job
  if (JOB_ID == T_JOB_ID) {
    T_BUILD_NUMBER = currentBuild.number
    T_JOB_NAME = env.JOB_NAME
    FORCE = params.FORCE
  }

  // log_dir is identified by JOB_NAME and BUILD_NUMBER
  mark_tag_name = "${T_JOB_NAME}_${T_BUILD_NUMBER}"
  log_dir += "/${mark_tag_name}"

  // if test_by_tag is specified, use it to mark the pipeline
  if (params.test_by_tag != null && params.test_by_tag.trim() != "none") {
    mark_tag_name = params.test_by_tag
  }
  VERSION_FILE = log_dir + "/version"

  if (JOB_ID == 'test_job') {
    run_bash """
      ulimit -S -c unlimited poc_gpe_server
      ulimit -S -c unlimited poc_rest_server
      ulimit -S -c unlimited ids_worker
      ulimit -S -c unlimited tg_dbs_gped
      ulimit -S -c unlimited tg_dbs_restd
      ulimit -S -c unlimited tg_dbs_gsed
    """
  }

  if (T_JOB_ID == 'HOURLY') {
    PARAM = ''
  }

  // it is test_job/build_job
  if (JOB_ID != T_JOB_ID) {
    currentBuild.description = NODE_NAME
    log_dir += "/${JOB_ID}_${currentBuild.number}_${NODE_IP}"
  }
}

def pre_pipeline() {
  echo "Start pre_pipeline()"
  USER_NOTIFIED = "${USER_NAME}@tigergraph.com"
  
  TEST_CONFIG = readJSON file: "${config_dir}/test_config.json"

  //comment out to avoid any unexpected modification
  //def tmp_config = readJSON file: "${test_config_folder}/test_config.json"
  //TEST_CONFIG << tmp_config

  if ( ! (MACHINE =~ "ec2") ) {
    sh """
      sudo service rpcbind start || true
      if [[ "${CONFIG['jenkins_id']}" == "prod_sv4"* ]]; then
        echo -e 'nameserver 1.1.1.1\nnameserver 8.8.8.8' | sudo tee /etc/resolv.conf
        if ! (mount | grep '/mnt/nfs_datapool' &>/dev/null); then
          sudo umount -lf /mnt/nfs_datapool &> /dev/null || true
          if ls /mnt/nfs_datapool/* &>/dev/null; then
            sudo mv /mnt/nfs_datapool /mnt/nfs_datapool.old || true
          fi
          sudo mkdir -p /mnt/nfs_datapool
          sudo mount -t nfs 192.168.99.8:/volume1/datapool /mnt/nfs_datapool || true
        fi
      fi
      if [[ "${CONFIG['jenkins_id']}" == *"gke"* ]]; then
        [[ ! \$(grep ftp.graphsql.com /etc/hosts) ]] && echo '${CONFIG['ftp_server_address']} ftp.graphsql.com' | sudo tee -a /etc/hosts;
        [[ ! \$(grep rdbms.graphtiger.com /etc/hosts) ]] && echo '${CONFIG['rdbms_server_address']} rdbms.graphtiger.com' | sudo tee -a /etc/hosts;
      fi
      if ! (ls /usr/lib64/libsasl2.so &>/dev/null); then
        if ls /usr/lib64/libsasl2.so* &>/dev/null; then
          sasl_file=\$(ls /usr/lib64/libsasl2.so* 2>/dev/null | head -1)
          sudo ln -sf \${sasl_file##*/} /usr/lib64/libsasl2.so
        fi
      fi
      find ${mit_workspace}/../ -maxdepth 1 -mindepth 1 -type d -mtime +2 | xargs sudo rm -rf
    """
  }

  USER_TO_NOTIFY = []
  for (def user in ALL_USER_NAME.split(",")) {
    try {
      USER_TO_NOTIFY.add(sendToServer("/users/${user.trim()}", 'GET')['result'][0]['email'])
    } catch (err) {
      USER_TO_NOTIFY.add("${user}@tigergraph.com")
      echo "Create New MIT User for ${user}"
      sendToServer("/users/${user}/register", 'GET')
    }
  }
  USER_NOTIFIED = USER_TO_NOTIFY.join(",")
  if (USER_NOTIFIED == "") {
    USER_NOTIFIED = "${USER_NAME}@tigergraph.com"
  }
  echo "User to notified is ${USER_NOTIFIED}"

  create_log_dir()
  check_log_url()
}

def before_test() {
  create_tmp_config()
}

def create_tmp_config() {
  test_config_file = "${log_dir}/mit_log/test_config.json"
  repo_list_file = "${log_dir}/mit_log/repo_list"
  def is_found = 0
  for (def r_info in TEST_CONFIG['branch_whitelist']) {
    if (BASE_BRANCH == r_info['name'] || BASE_BRANCH ==~ /tg_${r_info['name']}\.[\d]+_(dev|oem)/) {
      if (r_info['branch'].any({BASE_BRANCH ==~ it})) {
        //white listed
        is_found = 2
      } else {
        //not-white listed
        is_found = 1
      }
      def tmp_r_info = r_info
      tmp_r_info.remove('name')
      tmp_r_info.remove('branch')
      TEST_CONFIG << tmp_r_info
      break
    }
  }
  TEST_CONFIG.remove('branch_whitelist')
  //println TEST_CONFIG
  def all_repos = ""
  for (def repo_info in TEST_CONFIG['dependency']) {
    all_repos = "${all_repos} ${repo_info.key}"
  }
  run_bash """
    echo "${all_repos}" > ${repo_list_file}
  """
  writeJSON file: test_config_file, json: TEST_CONFIG, pretty: 4
  if (JOB_ID == T_JOB_ID ) {
    if (is_found == 0) {
      do_notification(PARAM, 'FAIL', ["Reason":
          "The base branch is not valid"])
      error("The base branch is not valid")
    } else if (is_found == 1 && T_JOB_ID != "HOURLY" && T_JOB_ID != "WIP" && FORCE == false) {
      //Restrict only MIT. Do not restrict hourly and WIP
      //Using FORCE = true for hourly would result in subsequent
      //hourly jobs not being blocked should one fail
      println "The base branch ${BASE_BRANCH} is not in white list"
      if (SPECIAL_APPROVED) {
        println "Special approval is found, so will not block"
      } else {
        //do_notification(PARAM, 'FAIL', ["Reason":
        //    "The base branch ${BASE_BRANCH} is not in white list"])
        error("The base branch ${BASE_BRANCH} is not in white list, please seek approval from release approvers")
      }
    } else if ((BASE_BRANCH ==~ /master/ || BASE_BRANCH ==~ /tg_[\d]+\.[\d]+_dev/) && T_JOB_ID != "HOURLY" && T_JOB_ID != "WIP" && FORCE == false) {
      //Internal release needs repo approval even it's while listed
      if (REPO_APPROVED || SPECIAL_APPROVED) {
        println "Repo/Release approval is found, so will not block"
      } else {
        error("The base branch ${BASE_BRANCH} needs repo approval, please seek approval from repository owners")
      }
    }
  }
}

/*
*  Instead of run comand one by one,
*  Combine all bash command together to run once.
*  Used for the command that has output redirection
*/
def run_bash(String command, Boolean showCmd = true) {
  if (showCmd) {
    echo command
  }
  sh "#!/bin/bash \n  ${command}"
}

/*
*  param: run_bash_mod is used to run command together.
*  get std output
*/
def run_cmd_get_stdout(String cmd, Boolean run_bash_mod = false, Boolean showCmd = true) {
  if (run_bash_mod) {
    if (showCmd) {
      echo cmd
    }
    cmd = "#!/bin/bash \n ${cmd}"
  }
  def result = sh(script: cmd, returnStdout: true)
  while (result.endsWith('\n')) {
    result = result.substring(0, result.length() - 1)
  }
  return result
}

/*
*  get std err
*/
def run_cmd_get_stderr(String cmd, Boolean run_bash_mod = false, Boolean showCmd = true) {
  if (run_bash_mod) {
    if (showCmd) {
      echo cmd
    }
    cmd = "#!/bin/bash \n ${cmd}"
  }
  def redirect_cmd = "${cmd} 3>&1 1>&2 2>&3"
  def result = sh(script: redirect_cmd, returnStdout: true)
  while (result.endsWith('\n')) {
    result = result.substring(0, result.length() - 1)
  }
  return result
}

/*
* runs the given command and gets the return code
*/
def run_cmd_get_rc(String cmd, Boolean run_bash_mod = false, Boolean showCmd = true) {
  if (run_bash_mod) {
    if (showCmd) {
      echo cmd
    }
    cmd = "#!/bin/bash \n ${cmd}"
  }
  def rc = sh(script: cmd, returnStatus: true)
  return rc
}

def check_log_url() {
  if (MACHINE =~ "ec2" ) {
    log_url = "https://qe-release-prebuild.s3.amazonaws.com/mitLogs/"+log_dir.replaceAll(".*/mitLogs/","")
  } else {
    if ( env.JENKINS_ID ) {
      log_url = "http://${CONFIG['log_review_machine']}/Log.php?directory=${log_dir}"
    } else {
      log_url = "http://${CONFIG['log_review_machine']}/"+log_dir.replaceAll(".*/mitLogs/","")
    }
  }
  echo "\"\033[31mYou can check all logs at ${log_url}" +
      "\n=================================================" +
      "=========================================================\033[0m\""
}

/*
*  send curl to MIT REST server to get http result
*/
def sendToServer(url_suffix, method, data = null) {
  echo "Reqeusted for ${url_suffix}"
  if ( ! ( env.JENKINS_ID ) && (method == "PUT" || method == "POST" || url_suffix =~ "nodeOnline" || url_suffix =~ "takeOffline") ) {
    echo "Skipped for non-stadard jenkins"
    return new JsonSlurper().parseText('{"error":false,"message":"","result":0}')
  }
  def url = "http://${CONFIG['rest_server_address']}/api"
  if (url_suffix != "") {
    url += url_suffix;
  }
  def res = ''
  if (method == "GET" || method == "DELETE") {
    res = run_cmd_get_stdout("curl -X ${method} ${url}")
  } else {
    res = run_cmd_get_stdout("curl -H 'Content-Type: application/json' -X ${method} "
        + " -d '${new JsonOutput().toJson(data)}' '${url}'")
  }
  return new JsonSlurper().parseText(res)
}

/*
*  send notification to Zulip user
*  sub_test == true means it is not in master, but in test_job/build_job
*/
def notification(String parameters, String state, String user_name,
    String stream_name, String topic_name, notify_dict) {
  if (JOB_ID == T_JOB_ID) {
    notify_dict["url"] = "http://${CONFIG['jenkins_public_ip']}:${CONFIG['jenkins_port']}/job/${env.JOB_NAME}/${currentBuild.number}"
    notify_dict["name"] = "[${JOB_ID}#${currentBuild.number}](${notify_dict["url"]}) on ${BASE_BRANCH}(${PARAM.replaceAll(";", " ").trim()})"
  } else {
    notify_dict["url"] = "http://${CONFIG['jenkins_public_ip']}:${CONFIG['jenkins_port']}/job/${env.JOB_NAME}/${currentBuild.number}"
    notify_dict["masterUrl"] = "http://${CONFIG['jenkins_public_ip']}:${CONFIG['jenkins_port']}/job/${T_JOB_NAME}/${T_BUILD_NUMBER}"
    notify_dict["name"] = "[${JOB_ID}#${currentBuild.number}](${notify_dict['url']}) " +
        " of [${T_JOB_ID}#${T_BUILD_NUMBER}](${notify_dict['masterUrl']}) on ${BASE_BRANCH}(${PARAM.replaceAll(";", " ").trim()})"
  }
  def cmd = """ python3 "${python3_script_dir}/notification.py" "${parameters}" "${state}" """ +
      """ "${user_name}" "${stream_name}" "${topic_name}" '${(new JsonOutput().toJson(notify_dict)).replaceAll("'","&apos;")}' """
  sh cmd
  if (state == "FAIL") {
    run_bash """
      [ -e ${log_dir}/failed_flag ] || touch ${log_dir}/failed_flag
    """
  }
}

def do_notification(String parameters, String state, notify_dict, String user_notified = "") {
  def notify_test_room = 'none'
  if (T_JOB_ID == 'HOURLY' || (T_JOB_ID == 'MIT' && state == "FAIL")) {
    notify_test_room = CONFIG['notify_test_room']
  } 
  def notify_topic_name = "none"
  if (T_JOB_ID == 'HOURLY') {
    notify_topic_name = CONFIG['notify_test_room']
  }
  if (T_JOB_ID == 'MIT' && state == "FAIL" && notify_dict.containsKey("Machine")) {
    notify_topic_name = 'MIT Test Failures'
  }
  if (user_notified.isEmpty()){
    user_notified = USER_NOTIFIED
  }
  notification(parameters, state, user_notified, notify_test_room, notify_topic_name, notify_dict)
}

/*
*  split the string to get unittests and integrations array
*/
def remove_total(str) {
  def groups = str.tokenize('#')
  def new_groups = []
  for (def group in groups) {
    new_groups.push(group.tokenize('@')[0])
  }
  return new_groups
}

/*
* make jenkins output hyperlink
*/
String getHyperlink(String url, String text) {
  hudson.console.ModelHyperlinkNote.encodeTo(url, text)
}

@NonCPS def getEntries(m) {m.collect {k, v -> [k, v]}}

def arrToStr(arr) {
  def res_str = ""
  for (def elem in arr) {
    if (res_str == "") {
      res_str = "${elem}"
    } else {
      res_str = "${res_str},${elem}"
    }
  }
  return res_str
}


def git_clone(repo, path, options) {
  echo "git clone ${repo} in ${path}"
  run_bash("""
    git clone -b ${BASE_BRANCH} --quiet --depth=1 ${options} \
      https://${env.MIT_GIT_USER}:${env.MIT_GIT_TOKEN}@github.com/TigerGraph/${repo}.git ${path}
  """, false)
}

def create_log_dir() {
  echo log_dir
  run_bash("""
    sudo mkdir -p '${log_dir}/mit_log'
    #sudo chown -R ${CONFIG['test_machine_user']}:${CONFIG['test_machine_user']} '${CONFIG['log_dir']}/${T_JOB_NAME}_${T_BUILD_NUMBER}'
    sudo chmod 777 -R ${log_dir}
    #sudo setfacl -d -m :rwx ${log_dir}
    'env' &> '${log_dir}/mit_log/environment_variables.log'
  """)
}

// get branch name
def get_branch_name (repo) {
  return run_cmd_get_stderr("""
    python3 "${python3_script_dir}/get_branch_name.py" "${PARAM}" "${repo}" "${BASE_BRANCH}"
  """)
}

def print_summary() {
  if (!fileExists(VERSION_FILE)) {
    return
  }
  def summary = sendToServer("/${JOB_ID.toLowerCase()}/${currentBuild.number}/summary?stages=all",
      'GET')['result'];
  //def summary = 'Temporary summary message';
  echo "${summary}"
  return summary
}

def print_err_summary() {
  def err_summary = sendToServer("/${JOB_ID.toLowerCase()}/${currentBuild.number}/summary?stages=failed",
      'GET')['result'];
  //def err_summary = 'Temporary failure message';
  return err_summary;
}

def conclude_summary() {
  echo 'conclude summary'
  run_bash("""
    bash ${shell_script_dir}/conclude_summary.sh ${log_dir}
  """, false)
}

def create_test_result_flag(String test_result) {
  run_bash """
    [ -e ${log_dir}/test_result_flag ] || touch ${log_dir}/test_result_flag
    echo "${JOB_ID.toLowerCase()}_test_${currentBuild.number} ${CLUSTER_TYPE} ${test_result}" >> ${log_dir}/test_result_flag
  """
}

def check_if_aborted() {
  //BUILD_URL are public IP in cloud, it may not be reachable by internal server.
  sleep 8
  def grep_info = 'It is aborted'
  def build_job_url = ( ! env.JENKINS_ID || env.JENKINS_ID =~ "prod_sv4") ? "${BUILD_URL}" : "http://${CONFIG['jenkins_ip']}:${CONFIG['jenkins_port']}/job/${env.JOB_NAME}/${currentBuild.number}/"
  def aborted = run_cmd_get_stdout("""
    actions=\$(curl --silent ${build_job_url}api/json | jq -r '.actions[]._class' | grep 'jenkins.model.InterruptedBuildAction')
    if [[ -n "\$actions" ]]; then
        echo '${grep_info}'
    fi
  """, true)
  echo aborted
  if (aborted.contains(grep_info)) {
    return true
  }
  return false
}

def resubmit_validate() {
  def with_test = run_cmd_get_stdout("""
    if [ -s "${log_dir}/integration_test_summary" ] || [ -s "${log_dir}/unit_test_summary" ];
    then
      echo "true"
    else
      echo "false"
    fi
  """)
  return with_test
}

def remove_word_in_str(str, word) {
  str = " " + str + " "
  str = str.replaceAll("\\b" + word + "(_\\S+){0,1}\\b", " ").replaceAll("\\s+", " ")
  return str.trim()
}

def check_gle_in_param() {
  def gle_found = false
  for (def pr in PARAM.tokenize(';')) {
    def pr_arr = pr.tokenize("#")
    if (pr_arr.size() == 2 && pr_arr[0].toLowerCase() == 'gle') {
      gle_found = true
      break
    }
  }
  return gle_found
}

def getTigergraphRoot() {
  if ( env.MIT_TG_VERSION ==~ "^2.*" ) {
    run_bash """
      cd /home
      [ ! -d tigergraph ] && sudo ln -s graphsql tigergraph && sudo chown ${CONFIG['test_machine_user']}:${CONFIG['test_machine_user']} tigergraph || :
      cd -
      [ ! -e /home/tigergraph/tigergraph/tigergraph_coredump ] && sudo mkdir -p /home/tigergraph/tigergraph/tigergraph_coredump || :
      sudo chmod 777 /home/tigergraph/tigergraph/tigergraph_coredump || :
    """
    return "/home/tigergraph/tigergraph";
  }

  run_bash """
    [ ! -e "/home/${CONFIG['test_machine_user']}/tigergraph/tigergraph_coredump" ] && sudo mkdir -p /home/${CONFIG['test_machine_user']}/tigergraph/tigergraph_coredump || :
    sudo chmod 777 /home/${CONFIG['test_machine_user']}/tigergraph/tigergraph_coredump || :
  """
  return "/home/"+CONFIG['test_machine_user']+"/tigergraph";
}

//combine ip list with srouce config file
//to generate the target config file
def clusterConfigGen(mList, version, tgtFile) {
  println "Enter clusterConfigGen"
  def jsonTgt = new JSONObject()

  println "Version in clusterConfigGen in ${version}"
  def major_version = version.tokenize(".")[0] as Integer
  println "Major version is ${major_version}"

  if (mList.size() < 1) {
    return false;
  }

  //write config file
  if (major_version >= 3) {
    //3.x.x and above

    //Basic Config Section
    jsonTgt["BasicConfig"] = [:]

    //Tigergraph User
    jsonTgt["BasicConfig"]["TigerGraph"] = [:]
    jsonTgt["BasicConfig"]["TigerGraph"]["Username"] = CONFIG['test_machine_user']
    jsonTgt["BasicConfig"]["TigerGraph"]["Password"] = CONFIG['test_machine_passwd']
    jsonTgt["BasicConfig"]["TigerGraph"]["SSHPort"] = 22

    //Root Directory
    def tigergraphRoot = getTigergraphRoot()
    jsonTgt["BasicConfig"]["RootDir"] = [:]
    jsonTgt["BasicConfig"]["RootDir"]["AppRoot"] = tigergraphRoot + "/app"
    jsonTgt["BasicConfig"]["RootDir"]["DataRoot"] = tigergraphRoot+ "/data"
    jsonTgt["BasicConfig"]["RootDir"]["LogRoot"] = tigergraphRoot + "/log"
    jsonTgt["BasicConfig"]["RootDir"]["TempRoot"] = tigergraphRoot + "/temp"

    //License Key
    println "Tigergraph version 3 (${major_version}) and above detected. Getting version 3 license..."
    jsonTgt["BasicConfig"]["License"] = "curl -s ftp://ftp.graphsql.com/lic/license3.txt".execute().text.trim();


    //Node List and Advanced Config Section for Cluster Installation
    if (mList.size() > 1) {

      //get node ips
      nodesTgt = []
      for (def i = 0; i < mList.size(); ++i) {
        def node_num = i + 1
        nodesTgt.add("m" + node_num + ":" +  mList[i].tokenize('_')[-1])
      }
    } else {
      //Set node list to localhost to make installer happy
      //Installer need m1 to be localhost to know it's local install

      //Node is Localhost
      nodesTgt = ['m1: 127.0.0.1']
    }

    //Node List
    jsonTgt["BasicConfig"]["NodeList"] = nodesTgt

    //Advanced Config
     jsonTgt["AdvancedConfig"] = [:]
    jsonTgt["AdvancedConfig"]["ClusterConfig"] = [:]

    //Login Config
    jsonTgt["AdvancedConfig"]["ClusterConfig"]["LoginConfig"] = [:]

    //Cluster Config
    jsonTgt["AdvancedConfig"]["ClusterConfig"]["LoginConfig"]["SudoUser"] = CONFIG['test_machine_user']
    if ( MACHINE =~ "ec2" ) {
      //EC2 Machine
      jsonTgt["AdvancedConfig"]["ClusterConfig"]["LoginConfig"]["Method"] = "K"
      jsonTgt["AdvancedConfig"]["ClusterConfig"]["LoginConfig"]["K"] = CONFIG['test_machine_key']
    } else {
      //Docker and Local VM
      jsonTgt["AdvancedConfig"]["ClusterConfig"]["LoginConfig"]["Method"] = "P"
      jsonTgt["AdvancedConfig"]["ClusterConfig"]["LoginConfig"]["P"] = CONFIG['test_machine_passwd']
    }

    //HA Cluster
    jsonTgt["AdvancedConfig"]["ClusterConfig"]["ReplicationFactor"] = 1
  } else {
    //2.x.x and Below

    //Get Node IPs
    def nodesTgt = [:]
    for (def i = 0; i < mList.size(); ++i) {
      def node_num = i + 1
      nodesTgt.put("m" + node_num, mList[i].tokenize('_')[-1])
    }

    //Basic TigerGraph Information
    jsonTgt["tigergraph.user.name"] = CONFIG['test_machine_user']
    jsonTgt["tigergraph.user.password"] = CONFIG['test_machine_passwd']
    jsonTgt["tigergraph.root.dir"] = getTigergraphRoot()

    //License key
    println "Tigergraph version 2 (${major_version}) and below detected. Getting version 2 license..."
    jsonTgt["license.key"] = "curl -s ftp://ftp.graphsql.com/lic/license.txt".execute().text.trim();

    //Node IPs
    jsonTgt["nodes.ip"] = nodesTgt

    //Login Info
    jsonTgt["nodes.login"] = [:]
    jsonTgt["nodes.login"]["ssh.port"] = "22"

    if ( MACHINE =~ "ec2" ) {
      //EC2 Machine
      jsonTgt["nodes.login"]["chosen.method"] = "K"
      jsonTgt["nodes.login"]["K"] = [:]
      jsonTgt["nodes.login"]["K"]["sudo.user.name"] = CONFIG['test_machine_user']
      jsonTgt["nodes.login"]["K"]["ssh.key.file"] = CONFIG['test_machine_key']
    } else {
      //Docker and Local VM
      jsonTgt["nodes.login"]["chosen.method"] = "P"
      jsonTgt["nodes.login"]["P"] = [:]
      jsonTgt["nodes.login"]["P"]["sudo.user.name"] = CONFIG['test_machine_user']
      jsonTgt["nodes.login"]["P"]["sudo.user.password"] = CONFIG['test_machine_passwd']
    }

    //HA Cluster
    jsonTgt["HA.option"] = [:]
    jsonTgt["HA.option"]["enable.HA"] = "false"
  }

  writeJSON file: tgtFile, json: jsonTgt, pretty: 4

  return true;
}

//create nodes via k8s api
def create_pods(num) {
  def pods_info
  pods_info = run_cmd_get_stdout("""
      python3 "${python3_script_dir}/create_k8s_pods.py" --num '${num}' --suffix 'm'
    """)
  return pods_info
}

//create nodes via k8s api
def env_setup(machine) {
  def machine_ip = machine.split("_")[1]
  def cmd = "echo '${CONFIG['rdbms_server_address']} rdbms.graphtiger.com' | sudo tee -a /etc/hosts && echo '${CONFIG['ftp_server_address']} ftp.graphsql.com' | sudo tee -a /etc/hosts"
  run_bash("""
      if ! nc -z ${machine_ip} 22 &>/dev/null; then sleep 30; fi
      sshpass -p ${CONFIG['test_machine_passwd']} ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no ${CONFIG['test_machine_user']}@${machine_ip} "${cmd}"
    """)
}

//get test os for hourly job, each time one os
def get_os_for_hourly() {
  res = run_cmd_get_stdout("""
      bash "${shell_script_dir}/get_os_for_hourly.sh"
    """)
  test_os = (res =~ "centos" || res =~ "ubuntu" ) ? "$res": "ubuntu20"
  return test_os
}

// get debug ssh proxy info
def get_debug_ssh_proxy() {
  def debug_ssh_proxy
  if (MACHINE =~ "_ext" && JOB_ID != "build_job") {
    debug_ssh_proxy = run_cmd_get_stdout("""
      python3 "${python3_script_dir}/get_debug_proxy.py"
    """)
  } else {
    debug_ssh_proxy = CONFIG['k8s_ssh_proxy']
  }
  return debug_ssh_proxy
}

def get_failure_label(errMessage) {
  def failure_label = ""
  if (T_JOB_ID == 'HOURLY') {
    failure_label += "QA_HOURLY_FAILURE"
  }
  if (errMessage =~ "^Component") {
    failure_label += (errMessage.split(' ').length > 4 )? " ut_${errMessage.split(' ')[4]}": " ut_fail"
  }
  if (errMessage =~ "^Integration") {
    failure_label += (errMessage.split(' ').length > 4 )? " it_${errMessage.split(' ')[3]}_${errMessage.split(' ')[4]}": " it_fail"
  }
  return failure_label
}

def get_failure_test(errSummary) {
  //errSummary example: cqrs_mit regress80 1.3 min (failed)
  if (! errSummary) {
    return "none"
  }

  def result = errSummary.split("\n")[0].replaceAll(" \\(failed\\)","")
  if (result) {
    return result
  } else {
    return "none"
  }
}

def create_jira(notify_dict, labels) {
  def jira_summary = "[QA-HQC] ${T_JOB_ID}#${T_BUILD_NUMBER} ${JOB_ID}#${currentBuild.number} Hourly Test failure for \"${notify_dict['Failed Test']}\""   // QA-2542
  def log = notify_dict['Log']
  def log_url = log[log.indexOf("(")+1..log.indexOf(")")-1]
  def instruction_url = notify_dict['Debug Instruction'][17..-2]
  def comment = notify_dict['Comment']
  def return_url = comment[comment.indexOf("(")+1..comment.indexOf(")")-1]
  def extend_url = return_url.split("reclaim")[0] + "renew/4"
  def jira_description = """
  *Test Info*:
   :P ${JOB_ID}#${currentBuild.number} of ${T_JOB_ID}#${T_BUILD_NUMBER} FAIL
   Reason: ${notify_dict['Reason']}
   Machine: ${notify_dict['Debug Machine']}
   Debug Instruction: ${instruction_url}
   Jenkins Job: ${notify_dict['url']}
   Test Log: ${log_url}
   Jenkins masterUrl: ${notify_dict['masterUrl']}
   Return Machines: ${return_url}
   Renew Machines: [extend 4 hours|${extend_url}]
  """
  def jira_fix_version = env.MIT_TG_VERSION
  def cmd = """ python3 ${python3_script_dir}/issue_manager.py create '${jira_summary}' '${jira_description}' "${jira_fix_version}" "${VERSION_FILE}" --labels "${labels}" """
  def result = run_cmd_get_stdout(cmd)
  echo result
}

def update_download_link() {
  download_tab = T_JOB_ID
  if ( IS_DEV == "true" ) {
    download_tab = "Developer"
  }

  UTIL.run_bash """
    if [ -f ${log_dir}/build_job_*/*offline.tar.gz ]; then
      cd ${log_dir}
      package_file=\$(ls build_job_*/*offline.tar.gz | tail -1 || true)
      download_url="ftp://mitnas.graphsql.com/datapool/${log_dir.drop(log_dir.indexOf('mitLogs'))}/\$package_file"
      download_url_s3="https://tigergraph-release-prebuild.s3.amazonaws.com/prebuild/\${package_file##*/}"
      gsqlclient_url_s3=\${download_url_s3/%offline.tar.gz/gsql_client.jar}
      display_name=\$(basename \$package_file)
      cat > /tmp/download.html << EOF
  <tr id="start_\$display_name">
    <td>${new Date()}</td><td><a id="${download_tab}_download" href="\$download_url"><h3>\$display_name<i class="fa fa-arrow-down sshdt-ra"></i></h3></a></td>
    <td><a id="${download_tab}_download_s3" href="\$download_url_s3"><h3>S3<i class="fa fa-arrow-down sshdt-ra"></i></h3></a></td>
    <td><a id="${download_tab}_gsqlclient_s3" href="\$gsqlclient_url_s3"><h3>GSQL Client<i class="fa fa-arrow-down sshdt-ra"></i></h3></a></td>
    <td><details><summary>Version Information</summary><pre><p style="color: Green; text-align: left;">
\$(cat ${VERSION_FILE})
    </p></pre></details></td>
  </tr>
  <!--end_\$display_name-->
EOF
      if grep 'id="${download_tab}"' ${CONFIG['log_dir']}/download.html 2>&1 >/dev/null; then
        sed '/id="${download_tab}"/r /tmp/download.html' ${CONFIG['log_dir']}/download.html > download.html || true
      else
        sed '/id="Enterprise"/r /tmp/download.html' ${CONFIG['log_dir']}/download.html > download.html || true
      fi
      sudo cp -f download.html ${CONFIG['log_dir']}/download.html
      cd -
    fi
  """
}

def get_download_links() {
  def package_name = UTIL.run_cmd_get_stdout("ls -td ${log_dir}/tigergraph*-offline.tar.gz 2>/dev/null | tail -1 | sed 's#.*/##g'")

  def local_package_url = package_name ? "ftp://mitnas.graphsql.com/datapool/${log_dir.drop(log_dir.indexOf('mitLogs'))}/${package_name}" : ""
  def cloud_package_url = package_name ? "https://tigergraph-release-prebuild.s3.amazonaws.com/prebuild/${package_name}" : ""
  def gsql_client_url = package_name ? cloud_package_url.replace("offline.tar.gz", "gsql_client.jar") : ""

  def links = [
    "local_package_url": local_package_url, 
    "cloud_package_url": cloud_package_url,
    "gsql_client_url": gsql_client_url
  ]

  echo "links: ${links}"
  
  return links
}

def get_version_info() {
  def version = ""
  try {
    def cmd = """cat ${log_dir}/../version"""
    version = run_cmd_get_stdout(cmd)
    echo "version: ${version}"
  } catch(err) {
    echo "version: ${err}"
  }

  return version
}

return this
