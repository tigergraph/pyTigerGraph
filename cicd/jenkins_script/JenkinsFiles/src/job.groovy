// import groovy library
import groovy.time.*
import groovy.json.*

//Use node name for test and hourly machines
if ( MACHINE =~ "^${CONFIG['labelPrefix']['test']}_"
  || MACHINE =~ "^${CONFIG['labelPrefix']['hourly']}_" || MACHINE =~ "^${CONFIG['labelPrefix']['k8s']}_" ){
  machineList = [NODE_NAME]
} else {
  machineList = [NODE_IP]
}

def pre_job() {
  // set up core_dump folder
  //sh """
//    bash ${shell_script_dir}/sync_time.sh >> ${log_dir}/mit_log/sync_time.log
//    rm ${product_dir}/src/gle/libs/* /*.jar || true
//  """
  sh """
    if cat /etc/*-release | grep Ubuntu &> /dev/null; then
      #fix flock in gtest driver failed to pass user-defined function issue
      if ls -l \$(which sh) | grep dash &> /dev/null; then
        sudo ln -sf bash /bin/sh
      fi
      if ! sudo apt list --installed | grep sshpass &> /dev/null; then
        sudo apt-get install -y sshpass
      fi
      sudo apt-get autoremove -y
    else
      if ! sudo yum list installed | grep sshpass &> /dev/null; then
        sudo yum install -y sshpass
      fi
      if [ "${SANITIZER}" == "asan" ]; then
        if [ "${JOB_ID}" == "build_job" ]; then
          if grep "release 8" /etc/system-release &> /dev/null; then
            sudo yum install -y libtsan llvm libasan6 libubsan --skip-broken
            sudo ln -sf libasan.so.6 /usr/lib64/libasan.so
            sudo ln -sf libubsan.so.1 /usr/lib64/libubsan.so
          elif grep "release 7" /etc/system-release &> /dev/null; then
            sudo yum install -y libasan5 libubsan1 libtsan llvm --skip-broken
            sudo ln -sf libasan.so.5 /usr/lib64/libasan.so
            sudo ln -sf libubsan.so.1 /usr/lib64/libubsan.so
	  else
            sudo yum install -y libtsan llvm libasan libubsan --skip-broken
            sudo ln -sf libasan.so.0 /usr/lib64/libasan.so
            sudo ln -sf libubsan.so.0 /usr/lib64/libubsan.so
          fi
	else
          echo "Please check asan lib in syspre"
	fi
      fi
    fi
  """
  env.NO_COLLECTION = 'false'
  env.NO_FAIL = NO_FAIL
  env.IS_DEV = params.IS_DEV
  def data = ["job_id": currentBuild.number, "status": "RUNNING",
      "start_t": new Date().format("yyyy-MM-dd HH:mm:ss"),
      "os": OS, "log_dir": log_dir, "edge_infos": [:]]
  def edge_name_prefix = ""

  echo "Current version before test is ${env.MIT_TG_VERSION}"

  if (JOB_ID == 'test_job') {
    //Take machine offline first for test job
    UTIL.sendToServer("/nodes/${NODE_NAME}/takeOffline", 'PUT', ["log_dir": log_dir + '/mit_log'
          , "offline_message": "${USER_NAME} ${T_JOB_ID}#${T_BUILD_NUMBER} " +
            "${JOB_ID}#${currentBuild.number} | ${PARAM} | Running Cluster Slave"])

    //Set debug env variable for debug mode so makeScheduler knows to make debug build for restpp ut
    if (params.DEBUG_MODE.toLowerCase() == "true") {
      env.DEBUG = true
    }
    
    if ( MACHINE_LIST != null && MACHINE_LIST.trim() != "" && MACHINE_LIST.trim() != "default" ) {
      machineList.addAll(MACHINE_LIST.split(','))
    }
    env.MACHINE_LIST = machineList.join(",")

    tgRoot = UTIL.getTigergraphRoot()
    if ( tgRoot?.trim()) {
      def rc = UTIL.run_cmd_get_rc("bash ${shell_script_dir}/uninstall_pkg.sh &>> ${log_dir}/mit_log/uninstall_pkg.log", true, true)
      if (rc != 0) {
        println "MIT uninstall failed!"
        error("MIT uninstall failed!")
      } else {
        println "MIT uninstall successful!"
      }
    }
    println "After uninstall"

    if ( MACHINE =~ 'MIT' ) {
      check_disk_usage(70)
      println "After check disk usage"
    }

    //Keep install logs
    install_log_root = "${log_dir}/install_logs"
    UTIL.run_bash("mkdir -p ${install_log_root}/${NODE_IP}")

    if (params.CLUSTER_TYPE == "cluster" && !(MACHINE =~ "^${CONFIG['labelPrefix']['k8s']}_") ) {
      if ( machineList.size() < cluster_nodes_num) {
        pipeline_name = "parallel_test"
        if (MACHINE =~ '^ec2') {
          pipeline_name = "${pipeline_name}_ec2"
        }
        if (MACHINE =~ '^gce') {
          pipeline_name = "${pipeline_name}_gce"
        }

        try {
          build job: "${pipeline_name}", parameters: [
            [$class: 'StringParameterValue', name: 'PARAM', value: PARAM],
            [$class: 'StringParameterValue', name: 'USER_NAME', value: USER_NAME],
            [$class: 'StringParameterValue', name: 'MACHINE', value: MACHINE],
            [$class: 'StringParameterValue', name: 'MACHINE_LIST', value: machineList.join(",")],
            [$class: 'StringParameterValue', name: 'BASE_BRANCH', value: BASE_BRANCH],
            [$class: 'StringParameterValue', name: 'TMD_BASE_BRANCH', value: TMD_BASE_BRANCH],
            [$class: 'StringParameterValue', name: 'UNITTESTS', value: UNITTESTS],
            [$class: 'StringParameterValue', name: 'INTEGRATION', value: INTEGRATION],
            [$class: 'StringParameterValue', name: 'T_JOB_ID', value: T_JOB_ID],
            [$class: 'StringParameterValue', name: 'T_JOB_NAME', value: T_JOB_NAME],
            [$class: 'StringParameterValue', name: 'T_BUILD_NUMBER', value: T_BUILD_NUMBER],
            [$class: 'StringParameterValue', name: 'OS', value: OS],
            [$class: 'StringParameterValue', name: 'SKIP_BUILD', value: SKIP_BUILD],
            [$class: 'StringParameterValue', name: 'PARALLEL_INDEX', value: "${PARALLEL_INDEX}#" + machineList.size()],
            [$class: 'StringParameterValue', name: 'NO_FAIL', value: NO_FAIL],
            [$class: 'StringParameterValue', name: 'DEBUG_MODE', value: DEBUG_MODE],
            [$class: 'StringParameterValue', name: 'SANITIZER', value: SANITIZER],
            [$class: 'StringParameterValue', name: 'T_TIMEOUT', value: T_TIMEOUT],
            [$class: 'BooleanParameterValue', name: 'skip_bc_test', value: params.skip_bc_test],
            [$class: 'StringParameterValue', name: 'test_by_tag', value: test_by_tag],
            [$class: 'StringParameterValue', name: 'CLUSTER_TYPE', value: CLUSTER_TYPE],
            [$class: 'StringParameterValue', name: 'IS_DEV', value: IS_DEV]
          ]
          currentBuild.result = 'SUCCESS'
          return
        } catch (err) {
          UTIL.run_bash("cp -rf ${tgRoot}/log ${install_log_root}/${NODE_IP} || true")
          def stage_error = "Sibling test job failed"
          currentBuild.result = 'FAILURE'
          error(stage_error)
        }
      }
    }
    if (params.CLUSTER_TYPE == "cluster" && MACHINE =~ "^${CONFIG['labelPrefix']['k8s']}_" && cluster_nodes_num > 1) {
      try {
        new_machines = UTIL.create_pods(cluster_nodes_num-1)
        println new_machines
        for (def node_name in new_machines.split(",")) {
          UTIL.env_setup(node_name)
        }
        if (new_machines != "") {
          machineList.addAll(new_machines.split(","))
        } 
        env.MACHINE_LIST = machineList.join(",")
      } catch (err) {
          def stage_error = "failed to create new machines via k8s api"
          currentBuild.result = 'FAILURE'
          error(stage_error)
      }
    }

    //allocate test cases
    data['unittests'] = UNITTESTS
    data['integrations'] = INTEGRATION
    edge_name_prefix = "test"
  } else {
    // add K8S build slave node vertex
    if (JOB_ID == 'build_job' && MACHINE =~ "^${CONFIG['labelPrefix']['k8s']}_") {
      UTIL.sendToServer("/nodes/${NODE_NAME}/takeOffline", 'PUT', ["log_dir": log_dir + '/mit_log'
          , "offline_message": "${USER_NAME} ${T_JOB_ID}#${T_BUILD_NUMBER} " +
            "${JOB_ID}#${currentBuild.number} | ${PARAM} | Building Packages"])
    }
    check_disk_usage(90)
    edge_name_prefix = "build"
  }

  //*_node_info edge goes from node to build/test job
  //Also add machine edges for test/hourly/k8s machines 
  if ( MACHINE =~ "^${CONFIG['labelPrefix']['test']}_" 
      || MACHINE =~ "^${CONFIG['labelPrefix']['hourly']}_" || MACHINE =~ "^${CONFIG['labelPrefix']['k8s']}_" ) {
    def machine_id_arr = []
    if (MACHINE =~ "^${CONFIG['labelPrefix']['k8s']}_" ) {
      machine_id_arr.add(["from_id": machineList[0], "to_id": currentBuild.number])
    } else {
      for (def machine in machineList) {
        machine_id_arr.add(["from_id": machine, "to_id": currentBuild.number])
      }
    }

    data['edge_infos']['slave_node'] = [
      "edge_name": "${edge_name_prefix}_node_info",
      "edge_data": machine_id_arr
    ]
  }

  //mwh_*_info edge goes from mwh_request to build_job/test_job
  data['edge_infos']['mwh_request'] = [
    "edge_name": "mwh_${edge_name_prefix}_info",
    "edge_data": [[
      "from_id": "${T_JOB_ID.toLowerCase()}${T_BUILD_NUMBER}_${env.JENKINS_ID}",
      "to_id": currentBuild.number
    ]]
  ]
  UTIL.sendToServer("/${JOB_ID.toLowerCase()}/withEdge", 'POST', data)

  if (JOB_ID == 'test_job') {
    println "Current version is ${env.MIT_TG_VERSION} before config"
    if (machineList.size() > 1) {
      UTIL.clusterConfigGen(machineList, env.MIT_TG_VERSION, "${log_dir}/mit_log/install_config.json");
      env.test_mode="cluster"
    } else {
      def host_ip = sh(script: "hostname -I | cut -d' ' -f1", returnStdout: true).trim()
      UTIL.clusterConfigGen(["${host_ip}"], env.MIT_TG_VERSION, "${log_dir}/mit_log/install_config.json");
      env.test_mode="single"
    }
  }

  print "End of Prejob."
}


def job_success(start_t) {
  //remove .gitconfig from machine. This must be done after test since machine
  //won't be able to clone tmd if this file is corrupt.
  UTIL.run_bash("sudo rm -rf ~/.gitconfig* &>> ${log_dir}/mit_log/cleanup.log")

  // QA-2553
  if (JOB_ID.toLowerCase() =~ "build_job") {
    def links = UTIL.get_download_links()
    def version_info = UTIL.get_version_info()

    def data = ['job_id': currentBuild.number, 'product_commit_information': version_info]
    UTIL.sendToServer("/${JOB_ID.toLowerCase()}/${currentBuild.number}", 'PUT', data + links)
  }

  if (JOB_ID == "build_job" && MACHINE =~ "^${CONFIG['labelPrefix']['k8s']}_" && params.BUILD_ONLY == "true" && params.NO_FAIL !="0") {
    //add build_only and no_fail enabled build job to debug mode for test purpose
    def cur_t = new Date().format("yyyy-MM-dd HH:mm:ss"), end_debug_t = new Date().toCalendar()
    def default_debug_time = CONFIG['default_debug_time']
    end_debug_t.add(Calendar.HOUR_OF_DAY, default_debug_time)
    end_debug_t = end_debug_t.getTime().format("yyyy-MM-dd HH:mm:ss")
    def data = ["status": "FAILURE", "end_t": cur_t, "message": "For build_only and no_fail enabled job, keep the instance"]
    data["debugger"] = USER_NAME
    data["debug_start"] = cur_t
    data["debug_end"] = end_debug_t
    data["debug_status"] = true
    UTIL.sendToServer("/${JOB_ID.toLowerCase()}/${currentBuild.number}", 'PUT', data)
  } else {
    def data = ["status": "SUCCESS", "end_t": new Date().format("yyyy-MM-dd HH:mm:ss"),
      "timecost": TimeCategory.minus(new Date(), start_t).toString()]
    UTIL.sendToServer("/${JOB_ID.toLowerCase()}/${currentBuild.number}", 'PUT', data)
  }
  UTIL.check_log_url()

  // take online once the job succeed
  // Also take online houly machines and k8s build pods
  if (JOB_ID == "test_job" || (JOB_ID == "build_job" && !(params.BUILD_ONLY == "true" && params.NO_FAIL !="0"))) {
    // for loop is needed to make sure all machines are taken online when a job running in
    // cluster mode succeed
    if (MACHINE =~ "^${CONFIG['labelPrefix']['k8s']}_") {
      UTIL.sendToServer("/nodes/${machineList[0]}/takeOnline", 'PUT', ["log_dir": log_dir + '/mit_log'])
    }
  } else {
    UTIL.sendToServer("/nodes/${NODE_NAME}/takeOnline", 'PUT', ["log_dir": log_dir + '/mit_log'])
  }
}

def job_failed(err) {
  //remove .gitconfig from machine. This must be done after test since machine
  //won't be able to clone tmd if this file is corrupt.
  UTIL.run_bash("sudo rm -rf ~/.gitconfig* &>> ${log_dir}/mit_log/cleanup.log")

  //echo "${err}"
  if (UTIL.check_if_aborted() == false) {
    def m1_ip = "${machineList[0].tokenize('_')[-1]}"
    def machine_ip_list = "${machineList[0]};"
    machine_ip_list += "(${CONFIG['test_machine_user']}:${CONFIG['test_machine_passwd']})"
    def ssh_info = UTIL.get_debug_ssh_proxy()
    if ( machine_ip_list =~ "${CONFIG['k8s_network']}" ) {
      machine_ip_list += "(Portal: [Web SSH](http://${CONFIG['web_ssh_proxy']}/ssh/host/${m1_ip}) or `ssh ${CONFIG['test_machine_user']}@${ssh_info}`)"
    }
    def instruction_url = "https://graphsql.atlassian.net/wiki/spaces/QA/pages/1931083777"
    def notify_dict = ["Reason": "${err.getMessage()}", "Debug Machine": machine_ip_list, "Debug Instruction": "[How to debug](${instruction_url})", "Log": "[Check all logs](${log_url})"]
    // add k8s build slave nodes into debug nodes
    if (JOB_ID == "test_job" || ( JOB_ID == "build_job" && params.BUILD_ONLY == "true")) {
        // update offline info for failure
        if (MACHINE =~ "^${CONFIG['labelPrefix']['k8s']}_") {
          UTIL.sendToServer("/nodes/${machineList[0]}/takeOffline", 'PUT', ["log_dir": log_dir + '/mit_log'
              , "offline_message": "${USER_NAME} ${T_JOB_ID}#${T_BUILD_NUMBER} " +
              "${JOB_ID}#${currentBuild.number} | ${PARAM} | Cluster Debugging"])
        } else {
          UTIL.sendToServer("/nodes/${NODE_NAME}/takeOffline", 'PUT', ["log_dir": log_dir + '/mit_log'
              , "offline_message": "${USER_NAME} ${T_JOB_ID}#${T_BUILD_NUMBER} " +
              "${JOB_ID}#${currentBuild.number} | ${PARAM} | Cluster Debugging"])
        }
        notify_dict["Comment"] = "The cluster is taken offline for you to debug. You can click " +
            "[here](http://${CONFIG['rest_server_public_address']}/api/${JOB_ID}/${currentBuild.number}/reclaim?user=${USER_NAME})" +
            " to return the cluster(take it online) immediately"
        notify_dict["Reason"] += " ${UTIL.print_err_summary()}"
    } else {
      notify_dict["Comment"] = "This is build job"
    }

    //only send notification for test jobs if it's master node, MACHINE_LIST.split(",").size() returns 1 even MACHINE_LIST is empty
    if (JOB_ID == 'test_job' && cluster_nodes_num > 1 && MACHINE_LIST.split(",").size() < cluster_nodes_num) {
      echo "Slave test node, skip notification!"
    } else {
      UTIL.do_notification(PARAM, 'FAIL', notify_dict)
      if (T_JOB_ID == 'HOURLY' && JOB_ID == "test_job" && env.JENKINS_ID =~ '^prod') {
        echo "creating jira issue ..."
        def err_summary = "${UTIL.print_err_summary()}"
        notify_dict.put("Failed Test", UTIL.get_failure_test(err_summary))
        labels = UTIL.get_failure_label(notify_dict["Reason"])
        UTIL.create_jira(notify_dict,labels)
      }
      //pass failure message to master job
      UTIL.run_bash("echo \"${err.getMessage()}\" | sudo tee ${log_dir}/../failed_flag")
    }

    def cur_t = new Date().format("yyyy-MM-dd HH:mm:ss"), end_debug_t = new Date().toCalendar()
    def default_debug_time = CONFIG['default_debug_time']
    if (T_JOB_ID == "HOURLY") {
      default_debug_time = default_debug_time * 3
    }
    end_debug_t.add(Calendar.HOUR_OF_DAY, default_debug_time)
    end_debug_t = end_debug_t.getTime().format("yyyy-MM-dd HH:mm:ss")
    def data = ["status": "FAILURE", "end_t": cur_t, "message": err.getMessage()]
    if (JOB_ID == "test_job" || ( JOB_ID == "build_job" && params.BUILD_ONLY == "true" && MACHINE =~ "^${CONFIG['labelPrefix']['k8s']}_" )) {
      data["message"] += " ${UTIL.print_err_summary()}"
      data["debugger"] = USER_NAME
      data["debug_start"] = cur_t
      data["debug_end"] = end_debug_t
      data["debug_status"] = true
    }
    UTIL.sendToServer("/${JOB_ID.toLowerCase()}/${currentBuild.number}", 'PUT', data)
    check_disk_usage(90)
    currentBuild.result = 'FAILURE'
  } else {
    if (MACHINE =~ "^${CONFIG['labelPrefix']['k8s']}_") {
      UTIL.sendToServer("/nodes/${machineList[0]}/takeOnline", 'PUT', ["log_dir": log_dir + '/mit_log'])
    } else {
      for (def machine in machineList) {
        UTIL.sendToServer("/nodes/${machine}/takeOnline", 'PUT', ["log_dir": log_dir + '/mit_log'])
      }
    }
    currentBuild.result = 'ABORTED'
  }
  UTIL.check_log_url()
}

def stage_gtestspace() {
  def stage_name = 'Gtestspace'
  stage (stage_name) {
    // set timeout 20 minutes
    timeout(time: 30, unit: 'MINUTES') {
      echo "${stage_name} starts"
      // if test_by_tag is specified, pass it to gworkspace script
      def test_by_tag = params.test_by_tag
      if (test_by_tag == null || test_by_tag.trim() == "none") {
        test_by_tag = ""
      }
      def file_tag = mark_tag_name
      if (SKIP_BUILD != "false") {
        file_tag = SKIP_BUILD
      }
      if ( MACHINE =~ "ec2" ) {
        //if running on ec2 machine
        UTIL.run_bash """
          mkdir -p ${product_dir}
          curl -s -L https://tigergraph-release-prebuild.s3.amazonaws.com/prebuild/${env.BASE_BRANCH}-${file_tag}-gtest.tar.gz -o ${product_dir}/gtest.tar.gz \
            &> ${log_dir}/gtest_unpackage.log
        """
      } else {
        //if running of kubernettes or local vm
        UTIL.run_bash """
          if [ -d ${product_dir} ]; then
            rm -rf ${product_dir}
          fi
          mkdir -p ${product_dir}
          cp -pf ${log_dir}/../build_*/*-gtest.tar.gz \
            ${product_dir}/gtest.tar.gz
        """
      }
      def rc = UTIL.run_cmd_get_rc("bash ${shell_script_dir}/gtest_manager.sh -a restore -t gtest.tar.gz -p ${product_dir} &> ${log_dir}/gtest_restore.log", true, true)
      if (rc != 0) {
        println "MIT gtestspace failed!"
        error("MIT gtestspace failed!")
      } else {
        date = new Date().format("yyyy-MM-dd HH:mm:ss")
        echo "${date} Finished copying gtest package and creating product directory environment"
        println "MIT gtestspace successful!"
      }
    }
  } // stage end
}

def stage_gworkspace() {
  def stage_name = 'Gworkspace'
  stage (stage_name) {
    // set timeout 20 minutes
    timeout(time: 120, unit: 'MINUTES') {
      echo "${stage_name} starts"
      // if test_by_tag is specified, pass it to gworkspace script
      def test_by_tag = params.test_by_tag
      if (test_by_tag == null || test_by_tag.trim() == "none") {
        test_by_tag = ""
      }
      //Only use achieve on build servers
      if (JOB_ID == "build_job"){
        if ( BUILD_ONLY == "k8s" || BUILD_ONLY == "scan") {
          BUILD_ONLY = "true"
        }
        // if release or prebuild
        if ( BUILD_ONLY == "release" || BUILD_ONLY == "prebuild" || BUILD_ONLY == "cloud" ) {
          if ( BUILD_ONLY == "cloud" ) {
            env.GUI_CLOUD_BUILD = "1"
          }
          UTIL.run_bash("echo ${BUILD_ONLY} > ${log_dir}/build_type")
          //set default release tag
          BUILD_ONLY = "release_${env.MIT_TG_VERSION}_${new Date().format("MM-dd-yyyy")}"
        }
        if ( BUILD_ONLY =~ "^release_" ) {
          env.release_tag = BUILD_ONLY
          if ( IS_DEV != "false" && IS_DEV != "true" && IS_DEV != "default" ) {
            env.release_tag = env.release_tag.replace(env.MIT_TG_VERSION, env.MIT_TG_VERSION+"-"+IS_DEV)
          }
        }
	if ( SANITIZER == "asan" ) {
          UTIL.run_bash("echo asan > ${log_dir}/build_type")
	}
      }
      def result = UTIL.run_cmd_get_stderr("""python3 "${python3_script_dir}/gworkspace.py" """ +
          """ "${log_dir}" "${VERSION_FILE}" "${mark_tag_name}" "${BASE_BRANCH}" "${repo_list_file}" """)
      if (result != "") {
        def stage_err = "${stage_name} failed"
        echo stage_err
        echo result
        error(stage_err)
      }
      if (T_JOB_ID == "HOURLY"){
        UTIL.run_bash """ bash ${shell_script_dir}/diff_commit_with_stable.sh "${log_dir}"
        """
      }
    }
  } // stage end
}

def stage_build() {
  def stage_name = 'Build Binary'
  stage (stage_name) {
    // set timeout 3 hour
    timeout(time: 180, unit: 'MINUTES') {
      try {
        echo "${stage_name} starts"
        def cpkg_option = ""
        def build_options = ""
        if (params.IS_DEV.toLowerCase() == "true") {
          cpkg_option = "-s"
        }
        if (params.IS_AMI.toLowerCase() == "true") {
          cpkg_option = "-a"
        }
        if ( BUILD_ONLY =~ "^release_" ) {
            echo "release tag: ${env.release_tag}"
            cpkg_option = "-t ${env.release_tag} ${cpkg_option}"
        }
        if (params.SANITIZER.toLowerCase() == "asan" && params.DEBUG_MODE.toLowerCase() == "true") {
          cpkg_option = "-u ${cpkg_option}"
        } else {
          if (params.SANITIZER.toLowerCase() != "none") {
            print "In if"
            build_options += "-s " + params.SANITIZER.toLowerCase().capitalize() + " "
          }
          if (params.DEBUG_MODE.toLowerCase() == "true") {
            build_options += "-d"
          }
        }
        UTIL.run_bash """
          if [[ "${BASE_BRANCH}" == "master" ]] || [[ -n "\${FORCE_GO_PROXY}" ]]; then
            if (curl -sfI --connect-timeout 1 ${CONFIG['go_proxy_server']} &> /dev/null); then export GOPROXY=${CONFIG['go_proxy_server']}; fi
            echo "Current GOPROXY is \$GOPROXY"
          fi
          set -e

          bash ${shell_script_dir}/add_build_option.sh -p ${product_dir} ${build_options} \
                &>${log_dir}/mit_log/add_build_option.log

          [ ! -f ${VERSION_FILE} ] && cd ${product_dir} && ./gworkspace.sh -c &> ${VERSION_FILE}
          ls -l ${product_dir} &>> ${log_dir}/cpkg.log
          cd ${product_dir} && ./cpkg.sh -jn -i ${mark_tag_name} ${cpkg_option} &>> ${log_dir}/cpkg.log
          cd ${product_dir}/cmake_build && make install
          ls -l ${product_dir} &>> ${log_dir}/cpkg.log
        """
      } catch (err) {
        echo "${err}"
        def stage_err = "${stage_name} failed"
        echo stage_err
        error(stage_err)
      }
    }
  } // stage end
}

def stage_packaging() {
  def stage_name = 'Offline Packaging'
  stage (stage_name) {
    // set timeout 1.5 hour
    timeout(time: 90, unit: 'MINUTES') {
      try {
        offline_package_std = "tigergraph-${env.MIT_TG_VERSION}-offline.tar.gz"
        gsqlclient_package_std = "tigergraph-${env.MIT_TG_VERSION}-gsql_client.jar"
        if ( BUILD_ONLY =~ "^release_" ) {
          //should we change MIT_TG_VERSION directly?
          if ( IS_DEV != "false" && IS_DEV != "true" && IS_DEV != "default" ) {
            offline_package_std = offline_package_std.replace(env.MIT_TG_VERSION, env.MIT_TG_VERSION+"-"+IS_DEV)
            gsqlclient_package_std = gsqlclient_package_std.replace(env.MIT_TG_VERSION, env.MIT_TG_VERSION+"-"+IS_DEV)
          }
          offline_package = offline_package_std
          gsqlclient_package = gsqlclient_package_std
          package_mode = "prebuild"
          if ( env.GUI_CLOUD_BUILD == "1") {
            offline_package = "tigergraph-${env.MIT_TG_VERSION}-${mark_tag_name}-cloud-offline.tar.gz"
            gsqlclient_package = "tigergraph-${env.MIT_TG_VERSION}-${mark_tag_name}-cloud-gsql_client.jar"
            //package_mode = "cloud"
          }
          gtest_package = ""
        } else {
          package_mode = "mit"
          if ( SANITIZER == "asan" ) {
            offline_package = "tigergraph-${env.MIT_TG_VERSION}-${mark_tag_name}-asan-offline.tar.gz"
            gsqlclient_package = "tigergraph-${env.MIT_TG_VERSION}-${mark_tag_name}-asan-gsql_client.jar"
            gtest_package = "tigergraph-${env.MIT_TG_VERSION}-${mark_tag_name}-asan-gtest.tar.gz"
          } else {
            offline_package = "tigergraph-${env.MIT_TG_VERSION}-${mark_tag_name}-offline.tar.gz"
            gsqlclient_package = "tigergraph-${env.MIT_TG_VERSION}-${mark_tag_name}-gsql_client.jar"
            gtest_package = "tigergraph-${env.MIT_TG_VERSION}-${mark_tag_name}-gtest.tar.gz"
          }
        }
        if ( env.MIT_TG_VERSION ==~ /^2\.[\d\.]+/ ) {
          auto_release_branch = "auto_release"
        } else {
          auto_release_branch = "master"
        }

	def download_url="${log_dir}/${offline_package}"
        withCredentials([usernamePassword(usernameVariable: 'AWS_ACCESS_KEY_ID', credentialsId: 's3cp', passwordVariable: 'AWS_SECRET_ACCESS_KEY')]) {
          UTIL.run_bash """
            if [ \"${package_mode}\" = \"mit\" ]; then
              bash ${shell_script_dir}/gtest_manager.sh -a backup -t ${gtest_package} -p ${product_dir} &> ${log_dir}/gtest_package.log
              cp ${product_dir}/${gtest_package} ${log_dir}
              rm -rf ${product_dir}/${gtest_package}
            fi
            bash ${product_dir}/generate_loader_dependency.sh &> ${log_dir}/generate_loader_dependency.log || true
            cd ${product_dir}/bigtest
            gcc_version=\$(head -1 ${log_dir}/cpkg.log | grep version | awk '{print \$NF}')
            git reset --hard && git prune && git fetch --all --prune && git checkout ${auto_release_branch} && git pull && git pull
            DOC_BRANCH=${mark_tag_name} GIT_TOKEN=\$MIT_GIT_TOKEN bash ${product_dir}/bigtest/certification_script/offline_package/build_offline_pkg.sh ${log_dir} ${env.MIT_TG_VERSION} ${offline_package_std} \
            ${IS_DEV} \"${BASE_BRANCH}\" ${mark_tag_name} ${package_mode} ${product_dir} ${mark_tag_name} \"file://${product_dir}/tigergraph.bin\" &> ${log_dir}/offline_package.log
            if [ \"${package_mode}\" = \"mit\" ]; then
              mv ${log_dir}/${offline_package_std} ${log_dir}/${offline_package}
            fi
            cp -f ${product_dir}/src/gle/bin/gsql_client.jar ${log_dir}/${gsqlclient_package}
          """

          UTIL.run_bash """
            if [ -f "${log_dir}/binaries.list" ]; then
              #TODO: handle extra files here
              tar -cvf ${log_dir}/extra_binaries.tgz \$(cat ${log_dir}/binaries.list | grep -v ^#)
              echo "Extra binary files copied"
            fi
          """

          UTIL.run_bash """
            LOADER_DEPENDENCY=loader_dependency.tar.gz
            if [ -f "${product_dir}/\${LOADER_DEPENDENCY}" ]; then
              aws s3 cp ${product_dir}/\${LOADER_DEPENDENCY} s3://tigergraph-release-prebuild/prebuild/loader_dependency/${BASE_BRANCH}/\${LOADER_DEPENDENCY} --acl public-read > /dev/null || true
            fi 
          """

          //if (params.BUILD_ONLY != null && params.BUILD_ONLY == "true" || MACHINE =~ "ec2" || MACHINE =~ "k8s") {
          if (MACHINE =~ 'ec2' || MACHINE =~ "k8s") {
            if ( BUILD_ONLY =~ "^release_" ) {
              package_url = UTIL.run_cmd_get_stdout("cat ${log_dir}/offline_package.log | grep DOWNLOADURL | tail -1 | sed 's#.*http#http#g'")
              download_url = "[here](${package_url})"
              UTIL.run_bash """
                gsqlclient_url=\$(echo ${package_url} | sed 's#https://dl.tigergraph.com#s3://tigergraph-release-download#g; s/offline/gsql_client/; s/tar.gz/jar/g')
                gsqlclient_checksum=${gsqlclient_package.replace('.jar', '.sha256sum')}
                sha256sum ${log_dir}/${gsqlclient_package} | xargs bash -c 'echo \$0 > \${1%.*}.sha256sum'
                aws s3 cp ${log_dir}/${gsqlclient_package} \${gsqlclient_url} --acl public-read > /dev/null
                aws s3 cp ${log_dir}/\${gsqlclient_checksum} \${gsqlclient_url/.jar/.sha256sum} --acl public-read > /dev/null
              """
            } else {
              UTIL.run_bash """
                aws s3 cp ${log_dir}/${offline_package} s3://tigergraph-release-prebuild/prebuild/${offline_package} --acl public-read > /dev/null
                aws s3 cp ${log_dir}/${gsqlclient_package} s3://tigergraph-release-prebuild/prebuild/${gsqlclient_package} --acl public-read > /dev/null
                aws s3 cp ${log_dir}/${gtest_package} s3://tigergraph-release-prebuild/prebuild/${gtest_package} --acl public-read > /dev/null
              """
              download_url = "[here](https://tigergraph-release-prebuild.s3.amazonaws.com/prebuild/${offline_package})"
              // append DOWNLOADURL for mit/wip packages to log file
              UTIL.run_bash("""echo "[DOWNLOADURL] https://tigergraph-release-prebuild.s3.amazonaws.com/prebuild/${offline_package}" >> ${log_dir}/offline_package.log""")
            }
          } else {
            //  UTIL.run_bash """
            //    curl --ftp-create-dirs -T ${log_dir}/${offline_package} ftp://192.168.11.10/product/prebuild/${offline_package}
            //    curl --ftp-create-dirs -T ${log_dir}/${gtest_package} ftp://192.168.11.10/product/test/${gtest_package}
            //  """
            download_url = "[here](ftp://mitnas.graphsql.com/datapool/${log_dir.drop(log_dir.indexOf('mitLogs'))}/${offline_package})"
            UTIL.run_bash("""echo "[DOWNLOADURL] ftp://mitnas.graphsql.com/datapool/${log_dir.drop(log_dir.indexOf('mitLogs'))}/${offline_package}" >> ${log_dir}/offline_package.log""")
          }
        }
        def notify_dict = ["Reason": "Build successful", "Comment": "Your binary is ready in [download site](http://${CONFIG['log_review_machine']}/download.html). Please click " + download_url
          + " to download it, also [nfs](file://${log_dir}/${offline_package})",]
        if (params.BUILD_ONLY != "true" && params.BUILD_ONLY != "false") {
          notify_dict["Reason"] = "${params.BUILD_ONLY} package build successful"
        }
        UTIL.do_notification(PARAM, 'BUILT', notify_dict)
        //}
      } catch (err) {
        echo "${err}"
        def stage_err = "${stage_name} failed"
        echo stage_err
        error(stage_err)
      } finally {
        //For debug build remove cmake_build so the next job that uses the builder
        //does not build debug version
        if (params.DEBUG_MODE.toLowerCase() == "true") {
          UTIL.run_bash """
            rm -rf ${product_dir}/cmake_build
          """
        }
      }
    }
  }
}

def stage_install() {
  def stage_name = 'Offline Installation'
  //wa for installation lag in container
  UTIL.run_bash """
    sync
  """
  stage (stage_name) {
    // set timeout 1 hour
    timeout(time: 90, unit: 'MINUTES') {
      try {
        echo "${stage_name} starts"
        def build_label = "build"
        def date = new Date().format("yyyy-MM-dd HH:mm:ss")
        echo "${date} Start copying installation package"
        def file_tag = mark_tag_name
        if (SKIP_BUILD != "false") {
          file_tag = SKIP_BUILD
        }
        if ( MACHINE =~ "ec2" ) {
          // NAS access unavailable
          UTIL.run_bash """
            curl -s -L https://tigergraph-release-prebuild.s3.amazonaws.com/prebuild/tigergraph-${env.MIT_TG_VERSION}-${file_tag}-offline.tar.gz -o ${product_dir}/tigergraph-offline.tar.gz
          """
        } else {
          // NAS access available
          UTIL.run_bash """
            rm -rf ${product_dir}/t*offline*
            #cp -pf ${log_dir}/../build_*/tigergraph*-offline.tar.gz ${product_dir}/tigergraph-offline.tar.gz
            ln -s ${log_dir}/../build_*/tigergraph*-offline.tar.gz ${product_dir}/tigergraph-offline.tar.gz
          """
        }
        date = new Date().format("yyyy-MM-dd HH:mm:ss")
        echo "${date} Finished copying installation package"
        try {
          UTIL.run_bash("bash ${shell_script_dir}/install_pkg.sh ${log_dir}/mit_log/install_config.json ${env.MIT_TG_VERSION} ${env.test_mode} &> ${log_dir}/install_pkg.log")
        } catch (err) {
          UTIL.run_bash("cp -rf ${tgRoot}/log ${install_log_root}/${NODE_IP} || true")
          for (def machine in param.MACHINE_LIST.split(',')) {
            def machine_ip = machine.tokenize('_')[-1]
            UTIL.run_bash("ln -s \$(ls -dt ../../*/install_logs/${machine_ip} | head -1 ) ./${machine_ip} || true")
          }
          throw err
        }
      } catch (err) {
        echo "${err}"
        def stage_err = "${stage_name} failed"
        echo stage_err
        error(stage_err)
      }
    }
  } // stage end
}

def stage_component_test() {
  def stage_name = 'Component unit test'
  stage (stage_name) {
    def timeout_t = 360
    if (SANITIZER != "none") {
      timeout_t = 420
    }
    if (T_JOB_ID == 'HOURLY') {
      timeout_t = 1000 //temporarily increase hourly timeout to workaround Ubuntu 14 timeout issue.
    }
    if (T_TIMEOUT?.isInteger()) { timeout_t = T_TIMEOUT.toInteger() }
    if (UNITTESTS != "none") {
      timeout(time: timeout_t, unit: 'MINUTES') {
        try {
          echo "${stage_name} starts"
          // if UNITTESTS is "none", it still will be passed into unittest_file.
          // because unittest_file will check UNITTESTS value by regular expression.
          def ut_test_opt = ""
  
          if (SANITIZER != "none") {
            ut_test_opt += "-sanitizer '${SANITIZER}'"
          }
  
          UTIL.run_bash """
            ${unittest_folder}/run.sh ${log_dir} -b '${mark_tag_name}' -u '${UNITTESTS}' ${ut_test_opt}
          """
        } catch (err) {
          echo "${err}"
          def stage_err = "${stage_name} failed"
          echo stage_err
          error(stage_err)
        }
      }
    } else {
      echo "Skipping Unit Test since UNITTESTS is none..."
    }
  } // stage end
}

def stage_integration_test() {
  def stage_name = 'Integration test'
  stage (stage_name) {
    def timeout_t = 360
    if (SANITIZER != "none") {
      timeout_t = 420
    }
    if (T_JOB_ID == 'HOURLY') {
      timeout_t = 1200 //temporaly increase timeout for itergration test.
    }
    if (T_TIMEOUT?.isInteger()) { timeout_t = T_TIMEOUT.toInteger() }
    if (INTEGRATION != "none") {
      timeout(time: timeout_t, unit: 'MINUTES') {
        try {
          echo "${stage_name} starts"
          def it_opts = ""
          // check if it is hourly and only one test job of hourly need to backup schema
          if (T_JOB_ID == 'HOURLY' && PARALLEL_INDEX == "ubuntu16 : 0") {
            it_opts += " -h "
          }
          // check if to skip gsql back compatibility test
          if (params.skip_bc_test != null && params.skip_bc_test == true) {
            it_opts += " -skip_bc "
          }
          UTIL.run_bash "${integration_folder}/run.sh ${log_dir} -i '${INTEGRATION}' ${it_opts}"
        } catch (err) {
          echo "${err}"
          def stage_err = "${stage_name} failed"
          echo stage_err
          error(stage_err)
        } finally {
          if (Integer.parseInt(NO_FAIL) >= 2 && fileExists(log_dir + '/really_fail_flag')) {
            UTIL.run_bash """
              rm -rf ${log_dir}/really_fail_flag
              ${shell_script_dir}/collector.sh ${log_dir} &> $log_dir/mit_log/collector.log
            """
            error("Tests Failed with no_fail option enabled")
          }
        }
      }
    } else {
      echo "Skipping Intergation Test since INTEGRATION is none..."
    }
  } // stage end
}

def check_disk_usage(def threshold) {
  def disk_usage = UTIL.run_cmd_get_stdout("""
    echo \$(df -Ph ~ | tail -1 | awk '{print \$5}' | cut -d'%' -f 1)
  """, true, false)
  echo "Space usage of /home/tigergraph folder is ${disk_usage}"
  if (Integer.parseInt(disk_usage) > threshold) {
    echo "Disk usage of /home/tigergraph larger than ${threshold}% !!!!!!"
    error("Disk usage of /home/tigergraph larger than ${threshold}% !!!!!!")
  }
  disk_usage = UTIL.run_cmd_get_stdout("""
    echo \$(df -Ph /tmp | tail -1 | awk '{print \$5}' | cut -d'%' -f 1)
  """, true, false)
  echo "Space usage of /tmp folder is ${disk_usage}"
  if (Integer.parseInt(disk_usage) > threshold) {
    echo "Disk usage of /tmp larger than ${threshold}% !!!!!!"
    error("Disk usage of /tmp larger than ${threshold}% !!!!!!")
  }
}

def check_gle_constants() {
  if (UTIL.check_gle_in_param()) {
    lock("check_gle_constants_${currentBuild.number}") {
      def check_gle_const_f = "${log_dir}/../mit_log/check_gle_constants.log"
      if (!fileExists(check_gle_const_f)) {
        UTIL.run_bash("""
          bash ${shell_script_dir}/check_gle_constants.sh \
              &> ${check_gle_const_f}
        """)
      }
    }
  }
}

return this
