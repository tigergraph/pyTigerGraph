

//update tmd branch
withCredentials([usernamePassword(credentialsId: 'qa-graphsql', usernameVariable: 'MIT_GIT_USER', passwordVariable: 'MIT_GIT_TOKEN')]) {
  if ( BUILD_ONLY =~ "^release" || BUILD_ONLY =~ "^prebuild" || BUILD_ONLY =~ "^gen_download") {
    //TODO: move privilege to database user info
    if ( USER_NAME != "jaya.rangavajhula" && USER_NAME != "chengbiao.jin" && USER_NAME != "wenbing.sun" ) {
      def stage_err = "Permission denied to build release/prebuild packages"
      UTIL = load("mit/jenkins_script/JenkinsFiles/src/util.groovy")
      UTIL.do_notification('', 'FAIL', ["Reason": stage_err], "${USER_NAME}@tigergraph.com,chengbiao.jin@tigergraph.com,jaya.rangavajhula@tigergraph.com")
      echo stage_err
      error(stage_err)
    }
  }
  def pr_branch = TMD_BASE_BRANCH
  def product_branch = BASE_BRANCH
  if ( PARAM.trim() != "") {
    PARAM = PARAM.replaceAll('=','#')
    for (def pull_req : PARAM.tokenize(';')) {
      def repo = pull_req.tokenize('#')[0], num = pull_req.tokenize('#')[1]
      if (repo ==~ /^(?i)tmd/) {
        pr_branch = sh(script: """python3 "mit/jenkins_script/python3_script/get_pr_branch_name.py" tmd ${num}""", returnStdout: true).trim()
      }
      if (repo ==~ /^(?i)product/) {
        product_branch = sh(script: """python3 "mit/jenkins_script/python3_script/get_pr_branch_name.py" product ${num}""", returnStdout: true).trim()
      } else {
        product_branch = BASE_BRANCH
      }
    }
    if ( pr_branch != TMD_BASE_BRANCH ) {
      checkout poll:false, scm: [
          $class: 'GitSCM',
          branches: [[name: pr_branch]],
          userRemoteConfigs: [[
              url: "https://github.com/TigerGraph/tmd.git",
              credentialsId: 'qa-graphsql'
          ]]
      ]
    }
  }
  if ( product_branch == "default" ) {
    product_branch = "master" //TODO: might need change
  }
  echo "Using product branch ${product_branch}"
  env.MIT_TG_VERSION = sh(script: """curl -s --fail "https://\${MIT_GIT_TOKEN}@raw.githubusercontent.com/TigerGraph/product/${product_branch}/product_version" """, returnStdout: true).trim()
}
