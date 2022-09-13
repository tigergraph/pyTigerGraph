#!/bin/bash

function usage() {
  echo "Usage $0 [-h] <-a add_user/remove_user/add_node/remove_node> <parameters>"
  echo "-h displays this message"
  echo "-a specifies the action to take. It must be add_user, remove_user, add_node or remove_node"
  echo
  echo "For Add User:"
  echo "Usage $0 -a add_user <-u user_name> <-e email> <-j jira_username> <-z zulip_username> <-w work_location>" 
  echo "-u specifies the user_name for the user to add"
  echo "-e specifies the email for the user to add"
  echo "-j specifies the jira username for the user to add"
  echo "-z specifies the zulip username for the user to add"
  echo "-w specifies the work location for the user to add"
  echo
  echo "For Remove User:"
  echo "Usage $0 -a remove_user <-u user_name>"
  echo "-u specifies the user_name for the user to remove"
  echo
  echo "For Add Node:"
  echo "Usage $0 -a add_node <-n node_name> <-s status> <-o offline_message> <-i node_ip>"
  echo "-n specifies the node_name for the node to add"
  echo "-s specifies the default status of the node"
  echo "-o specifies the default offline message of the node"
  echo "-i specifies the ip address of the node"
  echo 
  echo "For Remove Node:"
  echo "Usage $0 -a remove_node <-n node_name>"
  echo "-n specifies the node_name for the node to remove"
}

SERVER_IP="192.168.55.21"
SERVER_PORT="8888"

#Read arguments
while getopts ":ha:u:e:j:z:w:n:s:o:i:" arg; do
  case "$arg" in
    h)
      usage
      exit 0;
      ;;
    a)
      action=${OPTARG}
      ;;
    u)
      user_name=${OPTARG}
      ;;
    e)
      email=${OPTARG}
      ;;
    j)
      jira_name=${OPTARG}
      ;;
    z)
      zulip_name=${OPTARG}
      ;;
    w)
      work_loc=${OPTARG}
      ;;
    n)
      node_name=${OPTARG}
      ;;
    s)
      node_status=${OPTARG}
      ;;
    o)
      node_message=${OPTARG}
      ;;
    i)
      node_ip=${OPTARG}
      ;;
    :)
      echo "Invalid option: $OPTARG requires an argument"
      usage
      exit 1
      ;;
    *)
      echo "Invalid option $arg"
      usage
      exit 1;
      ;;
  esac
done

if [[ -z $action ]]; then
  echo "Error: please specify \"add_user\", \"remove_user\", \"add_node\" or \"remove_node\" with the -a flag!"
  usage
  exit 1;
fi

action_lower=$(echo "$action" | tr '[:upper:]' '[:lower:]')

if [[ $action_lower = "add_user" ]]; then
  
  if [[ -z $user_name || -z $email || -z $jira_name || -z $zulip_name || -z $work_loc ]]; then
    echo "Error: parameters \"user_name\", \"email\", \"jira_name\", \"zulip_name,\" and \"work_loc\" are required for adding a user! 
    Please specify them with the -u, -e, -j, -z, and -w flags respectively!"
    usage
    exit 4;
  fi  

  response=$(curl -H "Content-Type: application/json" -X POST http://$SERVER_IP:$SERVER_PORT/api/users --data \
    "{\"user_name\":\"$user_name\", \"email\":\"$email\", \"jira_name\":\"$jira_name\", \"hipchat_name\":\"$zulip_name\", \"work_loc\":\"$work_loc\"}")
  
  if [[ $? = 0 ]]; then
    echo "User $user_name added successfully"
    exit 0
  else
    echo "Failed to add user $user_name Error: $response"
    exit 4
  fi

elif [[ $action_lower = "remove_user" ]]; then
  
  if [[ -z $user_name ]]; then
    echo "Error: parameter \"user_name\" is required for removing a user! Please specify it with the -u flag!"
    usage
    exit 4;
  fi

  response=$(curl -X DELETE http://192.168.55.21:8888/api/users/"$user_name")
  
  if [[ $? = 0 ]]; then
    echo "User $user_name removed successfully"
    exit 0
  else
    echo "Failed to remove user $user_name Error: $response"
    exit 4
  fi

elif [[ $action_lower = "add_node" ]]; then

  if [[ -z $node_name || -z $node_status || -z $node_message || -z $node_ip ]]; then
    echo "Error: parameters \"node_name\", \"node_status\", \"offline_message\" and \"node_ip\" are required for adding a node! 
    Please specify them with the -n, -s, -o and -i flags respectively!"
    usage
    exit 4;
  fi

  if ! [[ $node_ip =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "Error: Invalid ip ${node_ip}! Ip address must be in the form *.*.*.* with * being numbers"
    usage
    exit 4;
  fi

  response=$(curl -H "Content-Type: application/json" -X POST  http://192.168.55.21:8888/api/nodes --data \
    "{\"node_name\":\"$node_name\", \"status\":\"$node_status\", \"offline_message\":\"$node_message\", \"ip\":\"$node_ip\"}")

  if [[ $? = 0 ]]; then
    echo "Node $node_name added successfully"
    exit 0
  else
    echo "Failed to add node $node_name Error: $response"
    exit 4
  fi

elif [[ $action_lower = "remove_node" ]]; then
  
  if [[ -z $node_name ]]; then
    echo "Error: parameter \"node_name\" is required for removing a node! Please specify it with the -n flag!"
    usage
    exit 4;
  fi

  response=$(curl -X DELETE http://192.168.55.21:8888/api/nodes/"$node_name")
  
  if [[ $? = 0 ]]; then
    echo "Node $node_name removed successfully"
    exit 0
  else
    echo "Failed to remove node $node_name Error: $response"
    exit 4
  fi
else
  echo "Error action $action is invalid. It must be add_user, remove_user, add_node or remove_node!"
  usage
  exit 3
fi
