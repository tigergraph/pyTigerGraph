// import groovy library
import groovy.time.*
once_tagged = false
gle_repo_path = "/tmp/gle"

def stage_QA_check() {
  def stage_name = "check QA issue existance"
  stage (stage_name) {
    // if JOB is merge_request_pipeline and FORCE = true and BUILD_USER is 'QA Duty', can skip QA check
    if (FORCE == false) {
      echo "${stage_name} starts"
      // check every 10 minutes
      def times_max = 24 * 6, interval = 10, counter = 0
      while (counter < times_max) {
        // get the stderr and check whether it has error
        def k8s_check_cmd = ( ! env.JENKINS_ID ) ? "export KUBECONFIG=~/.kube/config-twt01 && kubectl get pods|grep k8s|wc -l" : "kubectl -n default get pods|grep k8s|wc -l"
        def resource_check = UTIL.run_cmd_get_stdout(k8s_check_cmd)
        echo "current pods: ${resource_check.toInteger()}"
        if (resource_check.toInteger() > CONFIG['k8s_max_pods'].toInteger()) {
          def stage_err = "It is in waiting list due to MIT Resources limitation."
          if (counter == 0) {
            echo "${stage_err}"
            UTIL.do_notification('', 'STATUS', ["Reason": stage_err])
          }
        } else {
          def result = UTIL.run_cmd_get_stderr("""python3 "${python3_script_dir}/issue_manager.py" check ${env.MIT_TG_VERSION}""")
          if (result != "") {
            def stage_err = "It is in waiting list due to QA Hourly Failure."
            if (counter == 0) {
              echo "${stage_err} ${result.trim()}"
              UTIL.do_notification('', 'STATUS', ["Reason": stage_err])
            }
          } else {
            break
          }
        }
        sleep interval * 60
        echo "Times: ${counter}, will sleep ${interval} minutes, then re-retry ..."
        counter += 1
      }
      if (counter == times_max) {
        def stage_err = "Timeout due to QA Hourly Failure Or MIT Resources limitation after 24 hours."
        UTIL.do_notification('', 'FAIL', ["Reason": stage_err])
        echo stage_err
        error(stage_err)
      }
    }
  } // stage end
}

def stage_validate(def check_merge=false) {
  def stage_name = "validation"
  stage (stage_name) {
    if (params.test_by_tag != null && params.test_by_tag.trim() != "none") {
      echo "No need to validate pull request because test_by_tag is specified"
      return;
    }
    echo "${stage_name} starts"
    def validate_state = "MIT"
    if (JOB_ID == 'WIP') {
      validate_state = 'WIP'
      // SET FORCE=ALL to require special approval for wip, allow force override
      //if ( FORCE == false ) {
      //  FORCE = 'ALL'
      //}
    }
    if (check_merge) {
      // Skip some checks during merge
      FORCE="MERGE"
    }
    println "base branch is ${BASE_BRANCH}"
    def tmp_base_branch_f = "/tmp/.${mark_tag_name}"
    lock('end2end_git'){
      def result = UTIL.run_cmd_get_stderr("""python3 "${python3_script_dir}/validate.py" """ +
          """ "${PARAM}" "${validate_state}" "${FORCE}" "${BASE_BRANCH}" "${tmp_base_branch_f}" """)
      if (result != "") {
        def stage_err = "${result.trim()}"
        echo stage_err
        if (result.startsWith("WARNING: ")) {
          UTIL.do_notification(PARAM, 'STATUS', ["WARNING": "${stage_err.substring(9)}"])
        } else {
          UTIL.do_notification(PARAM, 'FAIL', ["Reason": stage_err])
          error(stage_err)
        }
      }
    }
    UTIL.run_bash("cp -f ${tmp_base_branch_f} ${log_dir}/mit_log/")
    BASE_BRANCH = UTIL.run_cmd_get_stdout("cat ${tmp_base_branch_f} | head -n 1")
    env.MIT_TG_VERSION = UTIL.run_cmd_get_stdout("cat ${tmp_base_branch_f} | head -n 2 | tail -1")
    def sap = UTIL.run_cmd_get_stdout("cat ${tmp_base_branch_f} |head -n 3 | tail -1")
    def rap = UTIL.run_cmd_get_stdout("cat ${tmp_base_branch_f} |head -n 4 | tail -1")
    JIRA_TICKETS = UTIL.run_cmd_get_stdout("cat ${tmp_base_branch_f} |head -n 5 |tail -1 && rm -rf ${tmp_base_branch_f}")
    if (sap.toLowerCase().trim() == "true" || USER_NAME == "chengbiao.jin") {
      SPECIAL_APPROVED = true
    }
    if (rap.toLowerCase().trim() == "true") {
      REPO_APPROVED = true
    }
    println "base branch is ${BASE_BRANCH}"
    println "Special approved is ${SPECIAL_APPROVED}"
    println "Repository approved is ${REPO_APPROVED}"
    println "Related JIRA tickets are \"${JIRA_TICKETS}\""
    env.BASE_BRANCH = BASE_BRANCH
    //if ( ! SPECIAL_APPROVED && params.BUILD_ONLY != "true" && BUILD_ONLY != "true" ) {
    //  error("MIT/WIP is locked for non-build-only jobs, please obtain special approval through PR(s)")
    //}
    if (JOB_ID == 'MIT' && ! (UNITTESTS =~ '^default' && INTEGRATION =~ '^default')) {
      error("MIT job with non-standard ut/it is not allowed!")
    }
  } // stage end
}

def stage_merge() {
  stage ('merge pull requests') {
    if ( env.JENKINS_ID && env.JENKINS_ID =~ "stg") {
      echo 'bypass the merge for staging environment'
    } else {
      // if FORCE is true and BUILD_USER is 'QA Duty', force push
      if (FORCE == false) {
        echo 'checking QA Hourly Status before merging starts'
        def result = UTIL.run_cmd_get_stderr("""python3 "${python3_script_dir}/issue_manager.py" check ${env.MIT_TG_VERSION}""")
        if (result != "") {
          def check_err = "Merge to base branch failed due to QA Hourly Failure.\n" +
              "        MIT will re-submit this job for you."
          echo check_err
          echo result
          resubmit_job()
          UTIL.do_notification('', 'FAIL', ["Reason": check_err])
          error(check_err)
        }
      }
      echo "merge pull requests starts"
      echo "wait for github update review status"
      sleep 5
      lock('end2end_git') {
        def res = UTIL.run_cmd_get_stderr("""python3 "${python3_script_dir}/merge_pull_request.py" """ +
            """ "${PARAM}" "${BUILD_URL}" "${VERSION_FILE}" """)
        if (res != "") {
          def merge_err = "Merge pull requests failed. <p>${res.trim()}</p>"
          echo merge_err
          UTIL.do_notification('', 'FAIL', ["Reason": merge_err])
          error(merge_err)
        }
        if ( JIRA_TICKETS != "" ) {
          try {
            UTIL.run_cmd_get_stderr("""python3 "${python3_script_dir}/issue_manager.py" update "${JIRA_TICKETS}" label "tg_${env.MIT_TG_VERSION} merged_${env.MIT_TG_VERSION}" """)
          } catch (err) {
            def notify_dict = ["Reason": "JIRA update failed", "Comment": "Please add label tg_${env.MIT_TG_VERSION} for ${JIRA_TICKETS}. cc @**Jaya Rangavajhula** @**CHENGBIAO JIN** "]
            UTIL.notification(PARAM, 'STATUS', USER_NOTIFIED, 'TigerGraph Testing Status', 'JIRA failure', notify_dict)
          }
        }
      }
      // start to check gle constants
      def check_gle_const_f = "${log_dir}/mit_log/check_gle_constants.log"
      if (UTIL.check_gle_in_param() && fileExists(check_gle_const_f)) {
        def check_sum_out = readFile file: check_gle_const_f
        if (check_sum_out.contains("not equal")) {
          def msg = "TigerGraphConstants.java is changed in ${BASE_BRANCH}"
          UTIL.notification('', 'STATUS', 'none', CONFIG['gle_token_room'], CONFIG['gle_token_room'], ["WARNING": msg])
        }
      }
    }
  }
}


def resubmit_job() {
  def param_str = PARAM.replaceAll("#", "%23")
  def resubmit_url = "http://${CONFIG['jenkins_account']}:${CONFIG['jenkins_pwd']}" +
      "@${CONFIG['jenkins_ip']}:${CONFIG['jenkins_port']}/job/${env.JOB_NAME}/buildWithParameters"
  sh """
    curl --fail -G -X POST "${resubmit_url}" \
        --data "TMD_BASE_BRANCH=${TMD_BASE_BRANCH}" \
        --data "USER_NAME=${ALL_USER_NAME}" --data "PARAM=${param_str}" --data "BASE_BRANCH=${BASE_BRANCH}" \
        --data-urlencode "UNITTESTS=${UNITTESTS}" --data-urlencode "INTEGRATION=${INTEGRATION}" \
        --data-urlencode "CLUSTER_TYPE=${CLUSTER_TYPE}" \
        --data-urlencode "SKIP_BUILD=${SKIP_BUILD}"
  """
}

def once_success(start_t, msg) {
  UTIL.sendToServer("/${JOB_ID.toLowerCase()}/${currentBuild.number}/nodeOnline", "GET")
  def timecost = TimeCategory.minus(new Date(), start_t).toString()
  def data = ["status": "SUCCESS", "end_t": new Date().format("yyyy-MM-dd HH:mm:ss"),
      "timecost": timecost]
  // ignore SANITIZER test, so it will not affect the data precision
  if (SANITIZER != "none" || DEBUG_MODE.toLowerCase() == "true" || Integer.parseInt(NO_FAIL) >= 2 || SKIP_BUILD != "false"
      || (INTEGRATION == "none" && UNITTESTS == "none")) {
    data["message"] = "ignore"
  }
  UTIL.sendToServer("/${JOB_ID.toLowerCase()}/${currentBuild.number}", 'PUT', data)
  if (binding.hasVariable('test_os_dict')) {
    UTIL.do_notification(PARAM, 'PASS', ["Result": msg, "timecost": timecost, "Instance Type": CLUSTER_TYPE, "Tested OS": "${UTIL.arrToStr(test_os_dict)}"])
  } else {
    UTIL.do_notification(PARAM, 'PASS', ["Result": msg, "timecost": timecost])
  }
  

  UTIL.conclude_summary()
  UTIL.print_summary()
  remove_mark_tag()
  if (JOB_ID == "MIT" && SANITIZER == "none" && DEBUG_MODE.toLowerCase() != "true") {
    calculate_time_cost()
  }

  currentBuild.result = 'SUCCESS'
  UTIL.create_test_result_flag('SUCCESS')

}

def once_failed(err) {
  UTIL.sendToServer("/${JOB_ID.toLowerCase()}/${currentBuild.number}/revertAll", 'GET')
  // only for wip/mit/hourly
  UTIL.sendToServer("/${JOB_ID.toLowerCase()}/${currentBuild.number}/nodeOnline", "GET")
  def data = ["status": "FAILURE", "end_t": new Date().format("yyyy-MM-dd HH:mm:ss")]
  def is_aborted = UTIL.check_if_aborted()
  if (is_aborted == false) {
    data["message"] = "${UTIL.print_err_summary()}"
    currentBuild.result = 'FAILURE'
  } else {
    data["status"] = "ABORTED"
    currentBuild.result = 'ABORTED'
  }
  def err_msg = UTIL.run_cmd_get_stdout("cat ${log_dir}/failed_flag 2>/dev/null || true")
  if ( err_msg == null || err_msg.isEmpty()) {
    err_msg = err.getMessage()
  }
  UTIL.sendToServer("/${JOB_ID.toLowerCase()}/${currentBuild.number}", 'PUT', data)
  if (binding.hasVariable('test_os_dict')) {
    UTIL.do_notification(PARAM, 'FAIL', ["Result": "Job failed! Error: ${err_msg}", "Instance Type": CLUSTER_TYPE, "Tested OS": "${UTIL.arrToStr(test_os_dict)}"])
  } else {
    UTIL.do_notification(PARAM, 'FAIL', ["Result": "Job failed! Error: ${err_msg}"])
  }

  UTIL.conclude_summary()
  UTIL.print_summary()
  UTIL.create_test_result_flag('FAILURE')
  remove_mark_tag()
  check_if_node_break()

  // do not resubmit if it is aborted
  def with_test = UTIL.resubmit_validate()
  if (CONFIG["auto_resubmit"].contains(JOB_ID) && SKIP_BUILD == "false" && is_aborted == false && with_test == "true"){
    resubmit_job()
  }
}

// Mark as stable
def tag_stable() {
  def tag_res = UTIL.run_cmd_get_stderr("""python3 "${python3_script_dir}/github_manager.py" """ +
      """ create "${repo_list_file}" tag "${CONFIG['qa_stable_tag']}_${BASE_BRANCH}" """ +
      """ file "${VERSION_FILE}" 2>> ${log_dir}/mit_log/github_manager.log """, true)
  if (tag_res != "") {
    def tag_err = "Tag stable failed."
    echo "${tag_err} ${tag_res.trim()}"
    UTIL.do_notification('', 'FAIL', ["Reason": tag_err])
    error(tag_err)
  }
  //create stable_version
  // env.MIT_TG_VERSION = "${BASE_BRANCH}".tokenize('_')[1]
  UTIL.run_bash("""
    cp -f "${VERSION_FILE}" "/mnt/nfs_datapool/mitLogs/config/stable_version_${env.MIT_TG_VERSION}"
  """)
}

def create_mark_tag() {
  //Create tag with $mark_tag_name
  if (once_tagged) {
    return
  }
  //STDOUT and STDERR are switched in run_cmd_get_stderr
  def tag_res = UTIL.run_cmd_get_stderr("""python3 "${python3_script_dir}/github_manager.py" """ +
    """ create "${repo_list_file}" tag "${mark_tag_name}" """ +
    """ params "${BASE_BRANCH}" "${PARAM}" 2> ${log_dir}/mit_log/github_manager_create.log """, true)
  if (tag_res != "") {
    def tag_err = "Tag params failed."
    echo "${tag_err} ${tag_res.trim()}"
    UTIL.do_notification('', 'FAIL', ["Reason": tag_err])
    error(tag_err)
  }
  once_tagged = true
}

def remove_mark_tag() {
  // if VERSION_FILE not exit, then gworkspace failed, so tag does not exist
  // if test_by_tag is specified, do not remove the specified tag.
  if (!once_tagged) {
    return
  }
  def tag_res = UTIL.run_cmd_get_stderr("python3 '${python3_script_dir}/github_manager.py' " +
      " delete '${repo_list_file}' tag '${mark_tag_name}' " +
      " params '${BASE_BRANCH}' '${PARAM}' 2> ${log_dir}/mit_log/github_manager_delete.log", true)
  if (tag_res != "") {
    def tag_err = "Failed to remove tag that marks commits for current pipeline"
    echo "${tag_err} ${tag_res.trim()}"
    UTIL.do_notification('', 'FAIL', ["Reason": tag_err])
    error(tag_err)
  }
}

def prebuild_check() {
  github_manager_log = "${log_dir}/mit_log/github_manager_create.log"
  withCredentials([[$class: 'AmazonWebServicesCredentialsBinding',credentialsId: 'prebuild_check',accessKeyVariable: 'AWS_ACCESS_KEY_ID',secretKeyVariable: 'AWS_SECRET_ACCESS_KEY']]) {
    UTIL.run_bash """
      bash ${shell_script_dir}/prebuild_check.sh "${github_manager_log}" "${CONFIG['rest_server_address']}" "${CONFIG['mit_server_address']}" "${USER_NOTIFIED}" "${env.BUILD_URL}" "${mark_tag_name}"
    """
  }

}

def calculate_time_cost() {
  UTIL.run_bash """
    python3 ${unittest_folder}/update_ut_timecost.py ${log_dir}/unit_test_summary \
        ${timecost_config_folder}/unittest_timecost.json
    python3 ${integration_folder}/update_it_timecost.py ${log_dir}/integration_test_summary \
        ${timecost_config_folder}/integration_timecost.json
  """
}

def check_if_node_break() {
  def grep_info = 'node break'
  def check_node = UTIL.run_cmd_get_stdout("""
    if [[ -f "${log_dir}/version" ]] && ! ls ${log_dir}/*/failed_flag &>/dev/null \
        && ! ls ${log_dir}/failed_flag &>/dev/null; then
      echo "${grep_info}"
    fi
  """, true)
  echo check_node
  if (check_node && check_node.contains(grep_info)) {
    def notify_dict = ["Reason": "Test machine might lose connections or jenkins pipeline was canceled",
        "Comment": "You can ask QA team for help"]
    UTIL.do_notification(PARAM, 'FAIL', notify_dict)
  }
}

def test_on_vm(os, unit_tests, integration_tests, test_id) {
  // running on the machine(s), default value is 'MIT'
  return {
    stage("parallel testing in ${os}") {
      def machine_name = "${CONFIG['labelPrefix']['test']}_${os}"
      if (MACHINE != 'MIT') {
        machine_name = "${MACHINE}_${os}"
      }
      pipeline_name = "parallel_test"
      if (MACHINE =~ 'ec2') {
        pipeline_name = "${pipeline_name}_ec2"
      }
      echo "unittests: ${unit_tests}"
      echo "integration tests: ${integration_tests}"
      def test_by_tag_tmp = "none"
      if (params.test_by_tag != null && params.test_by_tag.trim() != "none") {
        test_by_tag_tmp = params.test_by_tag
      }
      //wait 30s*N to avoid conflicts: 10s to get a new job and 20s to bring up child job.
      def wait_time = test_id.split(':').last().toInteger()
      sleep(wait_time*30) //seconds
      try {
         build job: "${pipeline_name}", parameters: [
           [$class: 'StringParameterValue', name: 'PARAM', value: PARAM],
           [$class: 'StringParameterValue', name: 'USER_NAME', value: ALL_USER_NAME],
           [$class: 'StringParameterValue', name: 'MACHINE', value: machine_name],
           [$class: 'StringParameterValue', name: 'BASE_BRANCH', value: BASE_BRANCH],
           [$class: 'StringParameterValue', name: 'TMD_BASE_BRANCH', value: TMD_BASE_BRANCH],
           [$class: 'StringParameterValue', name: 'UNITTESTS', value: unit_tests],
           [$class: 'StringParameterValue', name: 'INTEGRATION', value: integration_tests],
           [$class: 'StringParameterValue', name: 'T_JOB_ID', value: JOB_ID],
           [$class: 'StringParameterValue', name: 'T_JOB_NAME', value: env.JOB_NAME],
           [$class: 'StringParameterValue', name: 'T_BUILD_NUMBER', value: "${currentBuild.number}"],
           [$class: 'StringParameterValue', name: 'OS', value: os],
           [$class: 'StringParameterValue', name: 'SKIP_BUILD', value: SKIP_BUILD],
           [$class: 'StringParameterValue', name: 'PARALLEL_INDEX', value: test_id],
           [$class: 'StringParameterValue', name: 'NO_FAIL', value: NO_FAIL],
           [$class: 'StringParameterValue', name: 'DEBUG_MODE', value: DEBUG_MODE],
           [$class: 'StringParameterValue', name: 'SANITIZER', value: SANITIZER],
           [$class: 'StringParameterValue', name: 'T_TIMEOUT', value: JOB_TIMEOUT],
           [$class: 'BooleanParameterValue', name: 'skip_bc_test', value: params.skip_bc_test],
           [$class: 'StringParameterValue', name: 'test_by_tag', value: test_by_tag_tmp],
           [$class: 'StringParameterValue', name: 'CLUSTER_TYPE', value: CLUSTER_TYPE],
           [$class: 'StringParameterValue', name: 'IS_DEV', value: params.IS_DEV]
         ]
      } catch (err) {
        def stage_err="Parelle test job failed for ${test_id}"
        //error(stage_err)
        throw err
      }
    }
  }
}

def build_on_vm() {
  // running on the machine(s), default value is 'MIT'
  UTIL.run_bash """
    'env' &> '${log_dir}/mit_log/environment_variables.log'
  """
  def os = "centos6"
  if ( BASE_BRANCH == "master" || env.MIT_TG_VERSION ==~ /^3\.[789]\.[\d]+/ || env.MIT_TG_VERSION ==~ /^4\.[\d\.]+/) {
    os = "centos7"
  }
  def build_machine = "${CONFIG['labelPrefix']['build']}_${os}"
  def test_by_tag_tmp = "none"
  if (params.test_by_tag != null && params.test_by_tag.trim() != "none") {
    test_by_tag_tmp = params.test_by_tag
  }
  //hourly needs to use MIT build machine
  if ( MACHINE != "MIT" && MACHINE != "hourly") {
    build_machine = "${MACHINE}_${build_machine}"
  }
  stage("build in ${build_machine}") {
    if (SKIP_BUILD.contains("_test_") || SKIP_BUILD == "false") {
      echo "use specified skip_build option: ${SKIP_BUILD}"
    } else {
      echo "check if build already exist with the same commits"
      UTIL.run_bash """
        commit=\$(cat ${log_dir}/mit_log/github_manager_create.log | grep "Fetched commits for branches" | tail -1)
        path=\$(pwd)
        cd ${log_dir}/../
        test_jobs=\$(grep "\${commit}" *_test_*/mit_log/github_manager_create.log  | cut -d '/' -f1 | sort -r)
        echo \$test_jobs > ${log_dir}/skip_build
        echo "no match job" > ${log_dir}/match_job
        #only asan and normal package has gtest package
        if [ "\$SANITIZER" = "asan" ]; then
          target_fmt="tigergraph-*-asan"
        else
          target_fmt="tigergraph-*"
        fi
        for test_job in \$test_jobs; do
          [ "\$test_job" = "${mark_tag_name}" ] && continue || true
          if [[ "${BUILD_ONLY}" = "release" || "${BUILD_ONLY}" = "prebuild" ]]; then
            if ! grep "enterprise-edition" ${log_dir}/../\$test_job/build_*/offline_package.log &> /dev/null; then
              continue
            fi
            if [[ "${IS_DEV}" != "false" && "${IS_DEV}" != "true" && "${IS_DEV}" != "default" ]]; then
              if ! (grep "enterprise-edition" ${log_dir}/../\$test_job/build_*/offline_package.log | grep "${IS_DEV}") &> /dev/null; then
                continue
              fi
            fi
          fi
          if [ "${BUILD_ONLY}" = "cloud" ] && ! grep "cloud-edition" ${log_dir}/../\$test_job/build_*/offline_package.log &> /dev/null; then
            continue
          fi
          if [ "${BUILD_ONLY}" = "release" -o "${BUILD_ONLY}" = "prebuild" -o "${BUILD_ONLY}" = "cloud" -o ! -L ${log_dir}/../\$test_job/build_* -a -f ${log_dir}/../\$test_job/build_*/\${target_fmt}-offline.tar.gz -a -f ${log_dir}/../\$test_job/build_*/\${target_fmt}-gtest.tar.gz ]; then
            [ ! -f ${log_dir}/../\$test_job/version ] && continue || true
            [ "\$SANITIZER" != "asan" -a -f ${log_dir}/../\$test_job/build_*/\${target_fmt}-asan-offline.tar.gz ] && continue || true
            export SKIP_BUILD=\$test_job
            echo \$test_job > ${log_dir}/match_job
            echo "use \$test_job to skip build"
            echo "commits: \${commit}"
            break
          fi
        done
      """
      def build_job = readFile "${log_dir}/match_job"
      build_job = build_job.trim()
      if (build_job.contains("_test_")){
        if (SKIP_BUILD == "binary") {
          SKIP_BUILD = "binary-${build_job}"
        } else {
          SKIP_BUILD = build_job
        }
        def match_job_path = "${log_dir}/../${build_job}"
        if (build_job.contains("mit") || build_job.contains("wip") ) {
          match_job_dir = "${match_job_path}"
        }
        if (build_job.contains("hourly")) {
          def match_job_test_flag = new File("${match_job_path}/test_result_flag")
          if (match_job_test_flag.exists()){
            def match_job_cluster_type = match_job_test_flag.readLines().get(0).split(" ")[1]
            def match_job_test_result = match_job_test_flag.readLines().get(0).split(" ")[2]
            if (match_job_test_result == "SUCCESS") {
              if (match_job_cluster_type == "cluster") {
                CLUSTER_TYPE = "single"
              } else {
                currentBuild.result = 'ABORTED'
                echo "Skip this job since same commits package is already successfully tested in ${build_job}."
              }
            } else {
                CLUSTER_TYPE = "${match_job_cluster_type}"
                match_job_dir = "${match_job_path}"
            }
          }
        }
      } else {
        SKIP_BUILD = "false"
      }
      def data = ["skip_build": SKIP_BUILD]
      UTIL.sendToServer("/${JOB_ID.toLowerCase()}/${currentBuild.number}", 'PUT', data)
    }

    if (SKIP_BUILD == "false" || SKIP_BUILD =~ "^binary-") {
      build job: 'build_test', parameters: [
        [$class: 'StringParameterValue', name: 'PARAM', value: PARAM],
        [$class: 'StringParameterValue', name: 'USER_NAME', value: ALL_USER_NAME],
        [$class: 'StringParameterValue', name: 'OS', value: os],
        [$class: 'StringParameterValue', name: 'MACHINE', value: build_machine],
        [$class: 'StringParameterValue', name: 'BASE_BRANCH', value: BASE_BRANCH],
        [$class: 'StringParameterValue', name: 'TMD_BASE_BRANCH', value: TMD_BASE_BRANCH],
        [$class: 'StringParameterValue', name: 'T_JOB_ID', value: JOB_ID],
        [$class: 'StringParameterValue', name: 'T_JOB_NAME', value: env.JOB_NAME],
        [$class: 'StringParameterValue', name: 'T_BUILD_NUMBER', value: "${currentBuild.number}"],
        [$class: 'StringParameterValue', name: 'NO_FAIL', value: NO_FAIL],
        [$class: 'StringParameterValue', name: 'DEBUG_MODE', value: DEBUG_MODE],
        [$class: 'StringParameterValue', name: 'SANITIZER', value: SANITIZER],
        [$class: 'StringParameterValue', name: 'test_by_tag', value: test_by_tag_tmp],
        [$class: 'StringParameterValue', name: 'IS_DEV', value: params.IS_DEV],
        [$class: 'StringParameterValue', name: 'IS_AMI', value: params.IS_AMI],
        [$class: 'StringParameterValue', name: 'BUILD_ONLY', value: params.BUILD_ONLY]
      ]
    } else {
      echo "Skip build with ${SKIP_BUILD}"
      //current dir is logs/job_id/build_job_id
      UTIL.run_bash """
        pwd
        skip_build_job=\$(ls -td ${log_dir}/../${SKIP_BUILD}/build_* 2>/dev/null | tail -1 || :)
        if [ -n "\$skip_build_job" ]; then
          ln -s \$skip_build_job ${log_dir}/build_job_from_${SKIP_BUILD}
          cp -f ${log_dir}/../${SKIP_BUILD}/version ${log_dir}/
          if [ -f ${log_dir}/../${SKIP_BUILD}/diff_stable ]; then
            cp -f ${log_dir}/../${SKIP_BUILD}/diff_stable ${log_dir}/diff_stable
          fi
        else
          echo "Build ${SKIP_BUILD} is not found!"
          exit 1
        fi
      """
      //SKIP_BUILD is valid, check whether upload is requested
      if ((MACHINE =~ 'ec2' || MACHINE =~ "k8s") && BUILD_ONLY != "release" && BUILD_ONLY != "prebuild" && BUILD_ONLY != "cloud") {
        def offline_package_name = UTIL.run_cmd_get_stdout("ls -td ${log_dir}/build_job_*/tigergraph*-offline.tar.gz 2>/dev/null | tail -1")
        if (offline_package_name.isEmpty()) {
          error("Package for skip build is not found!")
        }
        def gsqlclient_package_name = offline_package_name.replace("offline.tar.gz", "gsql_client.jar")
        def offline_package = UTIL.run_cmd_get_stdout("echo ${offline_package_name} | sed 's#.*/##g'")
        withCredentials([usernamePassword(usernameVariable: 'AWS_ACCESS_KEY_ID', credentialsId: 's3cp', passwordVariable: 'AWS_SECRET_ACCESS_KEY')]) {
          UTIL.run_bash """
            if ! curl -sIL https://tigergraph-release-prebuild.s3.amazonaws.com/prebuild/${offline_package} | grep "200 OK" > /dev/null; then
              aws s3 cp ${offline_package_name} s3://tigergraph-release-prebuild/prebuild/${offline_package} --acl public-read > /dev/null
              if [ -f ${gsqlclient_package_name} ]; then
                aws s3 cp ${gsqlclient_package_name} s3://tigergraph-release-prebuild/prebuild/${offline_package.replace("offline.tar.gz", "gsql_client.jar")} --acl public-read > /dev/null
              fi
            fi
          """
        }
        download_url = "[here](https://tigergraph-release-prebuild.s3.amazonaws.com/prebuild/${offline_package})"
        def notify_dict = ["Reason": "Build is skipped successful", "Comment": "Your package is ready. Please click " + download_url
          + " to download it."]
        UTIL.do_notification(PARAM, 'BUILT', notify_dict)
      }
    }
  }
}

def stage_build_pkg() {
  def stage_name = "build package"
  stage( stage_name ) {
    // check throttle
    if (FORCE == false && JOB_ID != 'HOURLY') {
      def opCount = UTIL.sendToServer("/users/${USER_NAME}/checkThrottle", 'GET')['result']
      def privileged = CONFIG['throttle_exempt'].containsKey(USER_NAME) && ( CONFIG['throttle_exempt'][USER_NAME] == 0 || opCount < CONFIG['throttle_exempt'][USER_NAME] )
      if ( opCount >= CONFIG['user_throttle'] && !privileged ) {
        def stage_err = "Your running mit/wip + debugging job can not be larger than " +
            "${CONFIG['user_throttle']}. You have ${opCount} now."
        echo stage_err
        UTIL.do_notification(PARAM, 'FAIL',
            ["Reason": stage_err, "Comment": "You can use mit -ls to check your running mit/wip" +
                ", and use mit -return node_ip to return your debugging node"])
        error(stage_err)
      }
    }
    // tag the pull request
    if (params.test_by_tag == null || params.test_by_tag.trim() == "none") {
      create_mark_tag()
    }
    // health check for dependencies before actually running build job
    try {
      prebuild_check()
    } catch (err) {
      echo "${err}"
      def stage_err = "prebuild  check failed"
      echo stage_err
      error(stage_err)
    }
    //generate changelog
    if ( BUILD_ONLY == "release" || BUILD_ONLY == "prebuild" ) {
      def result = UTIL.run_cmd_get_stderr("""python3 "${python3_script_dir}/gen_changelog.py" """ +
        """ "${BASE_BRANCH}" "\$(cat ${repo_list_file})" &> ${log_dir}/changelog_${BASE_BRANCH}.md """)
      if (result != "") {
        def stage_err = "changelog generation failed"
        echo stage_err
        echo result
        error(stage_err)
      }
    }

    def pull_req_arr = [];
    PARAM = PARAM.replaceAll('=','#')

    //repo_request_info goes from repo to mwh_request
    for (def pull_req : PARAM.tokenize(';')) {
      def repo = pull_req.tokenize('#')[0], num = pull_req.tokenize('#')[1];
      pull_req_arr.add([
        "from_id": repo,
        "to_id": currentBuild.number,
        "pullreq": num.isInteger()?Integer.parseInt(num):0
      ])
    }

    def data = ["job_id": currentBuild.number, "job_type": JOB_ID.toLowerCase(), "status": "RUNNING",
        "start_t": new Date().format("yyyy-MM-dd HH:mm:ss"), "force": FORCE, "pullreq": PARAM,
        "unittests": UNITTESTS, "integrations": INTEGRATION, "base_branch": BASE_BRANCH,
        "bigtest_base_branch": TMD_BASE_BRANCH, "skip_build": SKIP_BUILD,
        "log_dir": log_dir, "edge_infos": [:]]

    //user_request_info edge goes from user to mwh_request
    data['edge_infos']["user"] = [
      "edge_name": "user_request_info",
      "edge_data": [[
        "from_id": USER_NAME,
        "to_id": currentBuild.number
      ]]
    ]

    data['edge_infos']["repo"] = [
      "edge_name": "repo_request_info",
      "edge_data": pull_req_arr
    ]
    UTIL.sendToServer("/${JOB_ID.toLowerCase()}/withEdge", 'POST', data)

    echo "Doing notification"
    // parallel testing start
    UTIL.do_notification(PARAM, 'START', [:])

    echo "Start building"
    // build stage
    try {
      build_on_vm()
      package_url = UTIL.run_cmd_get_stdout("cat ${log_dir}/build_job_*/offline_package.log | grep DOWNLOADURL | tail -1 | sed 's#.*http#http#g'")
      gsqlclient_url= package_url.replace("offline","gsql_client").replace(".tar.gz",".jar")
      echo "[DOWNLOADURL] ${package_url}"
      if (BUILD_ONLY == "release" && package_url =~ "enterprise-edition") {
        //should be s3 url in tigergraph-release-download bucket
        //https://dl.tigergraph.com/enterprise-edition/prebuild/tigergraph-3.6.1-offline-06280004.tar.gz
	s3_uri = package_url.replace("https://dl.tigergraph.com", "s3://tigergraph-release-download")
        release_package = package_url.replaceAll(/offline-[0-9]+\.tar\.gz/, "offline.tar.gz").replace("enterprise-edition/prebuild", "enterprise-edition")
	release_uri = release_package.replace("https://dl.tigergraph.com", "s3://tigergraph-release-download")
        gsqlclient_uri = gsqlclient_url.replaceAll(/gsql_client-[0-9]+\.jar/, "gsql_client.jar").replace("enterprise-edition/prebuild", "enterprise-edition").replace("enterprise-edition","enterprise-edition/gsql_client").replace("https://dl.tigergraph.com", "s3://tigergraph-release-download")
        gsqlclient_s3_uri = gsqlclient_url.replace("https://dl.tigergraph.com", "s3://tigergraph-release-download")

        withCredentials([usernamePassword(usernameVariable: 'AWS_ACCESS_KEY_ID', credentialsId: 's3cp', passwordVariable: 'AWS_SECRET_ACCESS_KEY')]) {
          UTIL.run_bash """
            if curl -sIL ${package_url} | grep "200 OK" > /dev/null; then
              aws s3 mv ${s3_uri} ${release_uri} --acl public-read > /dev/null
              aws s3 mv ${s3_uri.replace(".tar.gz",".sha256sum")} ${release_uri.replace(".tar.gz",".sha256sum")} --acl public-read > /dev/null
              aws s3 mv ${gsqlclient_s3_uri} ${gsqlclient_uri} --acl public-read > /dev/null
              aws s3 mv ${gsqlclient_s3_uri.replace(".jar", ".sha256sum")} ${gsqlclient_uri.replace(".jar",".sha256sum")} --acl public-read > /dev/null
            fi
          """
          ws {
            withCredentials([file(credentialsId: 'release_key', variable: 'KEY_FILE')]) {
              withCredentials([file(credentialsId: 'release_pass', variable: 'PASS_FILE')]) {
                UTIL.run_bash """
                  release_package=\$(echo ${release_package} | sed 's#.*/##g'")
                  curl -sOL ${release_package} > /dev/null
                  gpg --import \$KEY_FILE > /dev/null
                  gpg --passphrase-file \$PASS_FILE --batch --output \${release_package}.sig --detach-sign \${release_package/}
                  aws s3 cp \${release_package/}.sig ${release_uri}.sig --acl public-read > /dev/null
                """
              }
            }
          }
        }
	package_url = release_package
      }
      tg_version = env.MIT_TG_VERSION
      if ( IS_DEV != "false" && IS_DEV != "true" && IS_DEV != "default" ) { 
        tg_version = env.MIT_TG_VERSION + "-" + IS_DEV
      }
      //tg_version = UTIL.run_cmd_get_stdout("cat ${log_dir}/version | grep version | head -1 | rev | cut -d' ' -f1 | rev")
      if (BUILD_ONLY =~ "^release" || BUILD_ONLY == "prebuild" || BUILD_ONLY == "k8s" || BUILD_ONLY == "scan") {
        docker_registry=""
        docker_cred="docker_hub"
        if (BUILD_ONLY == "release") {
          build_type="release"
          tg_tag=tg_version
          dev_mode="external"
        } else {
          build_type="prebuild"
          tg_tag="${tg_version}-${mark_tag_name}"
          dev_mode="internal"
          //docker_registry="https://docker.tigergraph.com"
          //docker_cred="docker_registry"
          if (BUILD_ONLY == "scan") {
            dev_mode="scan"
            NO_FAIL="1" //skip k8s image build
          }
        }
        if (NO_FAIL == "0" || NO_FAIL == "1") {
          def docker_builds = [:]
          if (BUILD_ONLY != "k8s") {
            def normal_docker = {
                build job: 'docker_package', parameters: [
                  [$class: 'StringParameterValue', name: 'bigtest_dir', value: '/home/graphsql/release_product/bigtest'],
                  [$class: 'StringParameterValue', name: 'bigtest_branch', value: 'master'],
                  [$class: 'StringParameterValue', name: 'log_dir', value: '/home/graphsql/offline_package_log'],
                  [$class: 'StringParameterValue', name: 'version', value: tg_tag],
                  [$class: 'StringParameterValue', name: 'source_url', value: package_url],
                  [$class: 'StringParameterValue', name: 'pkg_name', value: 'mit'],
                  [$class: 'StringParameterValue', name: 'target_pkg', value: 'default'],
                  [$class: 'StringParameterValue', name: 'developer', value: dev_mode],
                  [$class: 'StringParameterValue', name: 'publish_package', value: build_type],
                  [$class: 'StringParameterValue', name: 'docker_registry', value: docker_registry],
                  [$class: 'StringParameterValue', name: 'docker_cred', value: docker_cred]
                ]
	    }
            docker_builds["normal_docker"] = normal_docker
          }
          if (NO_FAIL == "0" || BUILD_ONLY == "k8s") {
            def k8s_docker = {
                build job: 'docker_package', parameters: [
                  [$class: 'StringParameterValue', name: 'bigtest_dir', value: '/home/graphsql/release_product/bigtest'],
                  [$class: 'StringParameterValue', name: 'bigtest_branch', value: 'master'],
                  [$class: 'StringParameterValue', name: 'log_dir', value: '/home/graphsql/offline_package_log'],
                  [$class: 'StringParameterValue', name: 'version', value: tg_tag],
                  [$class: 'StringParameterValue', name: 'source_url', value: package_url],
                  [$class: 'StringParameterValue', name: 'pkg_name', value: 'mit'],
                  [$class: 'StringParameterValue', name: 'target_pkg', value: 'k8s'],
                  [$class: 'StringParameterValue', name: 'developer', value: dev_mode],
                  [$class: 'StringParameterValue', name: 'publish_package', value: build_type],
                  [$class: 'StringParameterValue', name: 'docker_registry', value: docker_registry],
                  [$class: 'StringParameterValue', name: 'docker_cred', value: docker_cred]
                ]
            }
            docker_builds["k8s_docker"] = k8s_docker
          }
          docker_builds['failFast'] = (Integer.parseInt(NO_FAIL) == 0)
          parallel docker_builds
        }
        if ( BUILD_ONLY == "release" || BUILD_ONLY == "prebuild" ){
          stage_gen_download()
        }
      }
    } catch (err) {
      echo "${err}"
      def stage_err = "${stage_name} failed"
      echo stage_err
      error(stage_err)
    }
  }
}

def stage_gen_download() {
  def stage_name = "generate download page"
  stage( stage_name ) {
    if ( IS_DEV == "false" || IS_DEV == "true" || IS_DEV == "default" ) {
      UTIL.run_bash """
        if [ -z "${package_url}" ]; then
          cp -f ${log_dir}/../config/release_config/tg_downloads.txt ${log_dir}/
        else
          if [ "${BUILD_ONLY}" == "release" ]; then
            sed -i '/Enterprise-Edition/a Enterprise|${env.MIT_TG_VERSION}|Enterprise Free Edition|||' ${log_dir}/../config/release_config/tg_downloads.txt 
            sed -i '/GSQL-Client/a GSQL Client|${env.MIT_TG_VERSION}|Enterprise Free Edition|||' ${log_dir}/../config/release_config/tg_downloads.txt 
            sed -i '/Docker-Image/a Docker|${env.MIT_TG_VERSION}|Enterprise Free Edition|||' ${log_dir}/../config/release_config/tg_downloads.txt 
            cp -f ${log_dir}/../config/release_config/tg_downloads.txt ${log_dir}/
          else
            gsqlclient_package=\$(echo ${package_url} | sed 's#https://dl.tigergraph.com#s3://tigergraph-release-download#g; s/offline/gsql_client/; s/tar.gz/jar/g')
            cp -f ${log_dir}/../config/release_config/tg_downloads.txt ${log_dir}/
            sed -i '/Enterprise-Edition/a Enterprise|${env.MIT_TG_VERSION}|Enterprise Free Edition|${package_url}||' ${log_dir}/tg_downloads.txt 
            sed -i "/GSQL-Client/a GSQL Client|${env.MIT_TG_VERSION}|Enterprise Free Edition|\${gsqlclient_package}||" ${log_dir}/tg_downloads.txt 
          fi
        fi
        bash mit/release_download/gen_download.sh -f ${log_dir}/tg_downloads.txt -o ${log_dir}/download.html > ${log_dir}/gen_download.log
      """
      withCredentials([usernamePassword(usernameVariable: 'AWS_ACCESS_KEY_ID', credentialsId: 's3cp', passwordVariable: 'AWS_SECRET_ACCESS_KEY')]) {
        UTIL.run_bash """
          if [ "${BUILD_ONLY}" = "release" ]; then
            aws s3api put-object --bucket tigergraph-release-download --key download.html --body ${log_dir}/download.html --cache-control "no-cache" --content-type "text/html" --acl bucket-owner-full-control --acl public-read
          fi
        """
      }
    }
  }
}

def stage_parallel_testing() {
  if (params.BUILD_ONLY != null && params.BUILD_ONLY != "false") {
      return
  }
  try {
    // parallel test stage
    // Initialized total_vm_num to 0 and not NUM_MACHINE parameter to avoid int/string
    // operation issues later (e.g. real_vm_num = total_vm_num * cluster_nodes_num).
    // String value will cause total_vm_num to be repeated cluster_node_num times.
    def total_vm_num = 0, tests_groups = []
    def to_run_UNITTESTS = "", to_run_INTEGRATION = ""

    def tmp_ut = UNITTESTS
    print "${UNITTESTS}"
    if (UNITTESTS =~ "^default" && (JOB_ID == 'HOURLY' || PARAM =~ "tmd#" ||
        (params.test_by_tag != null && params.test_by_tag.trim() != "none"))) {
      tmp_ut = UNITTESTS.replace("default","all")
    }
    all_to_run_UNITTESTS = UTIL.run_cmd_get_stdout("""
      python3 "${unittest_folder}/get_unittests.py" "${test_config_file}" "${PARAM}" "${tmp_ut}" "${match_job_dir}"
    """)
    to_run_UNITTESTS = UTIL.run_cmd_get_stdout("""
      python3 "${integration_folder}/get_integrations.py" -u "${all_to_run_UNITTESTS}" -cleanup
    """)
    print "${to_run_UNITTESTS}"
    if (!all_to_run_UNITTESTS.contains("its") && !(all_to_run_UNITTESTS =~ "E2E") && tmp_ut == "default") {
      println 'skip integration test due to dependency'
      to_run_INTEGRATION = "none"
    } else {
      if (UNITTESTS =~ " -") {
        all_to_run_UNITTESTS += " -" + UNITTESTS.split(" -")[-1]
      }
      to_run_INTEGRATION = UTIL.run_cmd_get_stdout("""
        python3 "${integration_folder}/get_integrations.py" -u "${all_to_run_UNITTESTS}" -i "${INTEGRATION}" -jp "${PARAM}" -bn "${mark_tag_name}" -mj "${match_job_dir}" 
      """)

      UTIL.run_cmd_get_stdout("""
        echo "all integrations tests: ${to_run_INTEGRATION}" &> ${log_dir}/mit_log/get_integrations.log
        str=\$(grep "all integrations tests: " ${log_dir}/mit_log/get_integrations.log)
        echo \${str##*all integrations tests: }
      """, true)
      print "${to_run_INTEGRATION}"
    }

    // test os mapping dict,centos6/ubuntu16 is eol, remove from list and add centos8
    if (JOB_ID != 'HOURLY') {
      if (BASE_BRANCH =~ "^tg_3[.]1[.]" || BASE_BRANCH =~ "^tg_2[.]") {
        test_os_dict = ['centos7', 'centos8', 'ubuntu16', 'ubuntu18']
      } else {
        test_os_dict =['centos7', 'centos8', 'ubuntu18', 'ubuntu20']
      }
      Collections.shuffle test_os_dict
    } else {
      test_os = UTIL.get_os_for_hourly()
      test_os_dict = [test_os, test_os, test_os, test_os]
    }

    if (JOB_ID != 'HOURLY') {
      def label_name = "${CONFIG['labelPrefix']['test']}"
      if ( MACHINE != "MIT") {
        label_name = MACHINE 
      }
      if (NUM_MACHINE != "default" && NUM_MACHINE.isInteger() && NUM_MACHINE.toInteger() <= 8 ) {
        total_vm_num = NUM_MACHINE.toInteger()
      } else {
        total_vm_num = 4
      }
      def tests_split_str = UTIL.run_cmd_get_stdout("""
        python3 "${python3_script_dir}/split_costs.py" "${timecost_config_folder}/unittest_timecost.json" \
            "${timecost_config_folder}/integration_timecost.json" "${to_run_UNITTESTS}" \
            "${to_run_INTEGRATION}"  ${total_vm_num} "${UTIL.arrToStr(test_os_dict)}" "${UTIL.arrToStr(special_ut_dict)}" \
            "${log_dir}/mit_log/group_tests.log"
      """)
      for (def group in tests_split_str.tokenize('#')) {
        tests_groups.push([
          "os": group.tokenize('$$$')[0].trim(),
          "ut": group.tokenize('$$$')[1].trim(),
          "it": group.tokenize('$$$')[2].trim()
        ])
      }
    } else {
      //Set number of machines for hourly to be dynamic to
      //support multiple hourly clusters
      if (NUM_MACHINE != "default" && NUM_MACHINE.isInteger()) {
        total_vm_num = NUM_MACHINE.toInteger()
      } else {
        total_vm_num = 4
      }
      if (FORCE == true) {
        for (def i = 0; i < total_vm_num; i++) {
          tests_groups.push([
            "os": test_os_dict[i % test_os_dict.size()],
            "ut": to_run_UNITTESTS,
            "it": to_run_INTEGRATION
          ])
        }
      } else {
        def tests_split_str = UTIL.run_cmd_get_stdout("""
        python3 "${python3_script_dir}/split_costs.py" "${timecost_config_folder}/unittest_timecost.json" \
            "${timecost_config_folder}/integration_timecost.json" "${to_run_UNITTESTS}" \
            "${to_run_INTEGRATION}"  ${total_vm_num} "${UTIL.arrToStr(test_os_dict)}" "${UTIL.arrToStr(special_ut_dict)}" \
            "${log_dir}/mit_log/group_tests.log"
        """)
        for (def group in tests_split_str.tokenize('#')) {
          tests_groups.push([
            "os": group.tokenize('$$$')[0].trim(),
            "ut": group.tokenize('$$$')[1].trim(),
            "it": group.tokenize('$$$')[2].trim()
          ])
        }
      }
    }
    println tests_groups

    def real_vm_num = total_vm_num
    // it is distributed cluster rather than single server
    if (CLUSTER_TYPE == "cluster") {
      real_vm_num = total_vm_num * cluster_nodes_num
    }
    UTIL.sendToServer("/${JOB_ID.toLowerCase()}/${currentBuild.number}", 'PUT', ["num_of_nodes": real_vm_num])

    // start parallel test
    def parallel_test = [:]
    parallel_test['failFast'] = (Integer.parseInt(NO_FAIL) == 0)
    for (def index = 0; index < tests_groups.size(); index++) {
      def this_os = tests_groups[index]["os"]
      def this_ut = tests_groups[index]["ut"]
      def this_it = tests_groups[index]["it"]
      if (this_ut == 'none' && this_it == 'none') {
        continue
      }
      if (this_os == "centos6") {
        for (def sut : special_ut_dict) {
          this_ut = UTIL.remove_word_in_str(this_ut, sut)
        }
      }
      def test_id = this_os + " : " + index;
      parallel_test[test_id] = test_on_vm(this_os, this_ut,
          this_it, test_id)
    }
    parallel parallel_test

    // back to master machine
  } catch (err) {
    def stage_err = "parallel test failed"
    echo stage_err
    error(stage_err)
  }
}

return this
