#!/usr/bin/python3                                                                                                                                    
import util
import sys

def main(parameters):
    repo = parameters[1]
    num = parameters[2]
    if num.isdigit():
      return util.get_pull_request_info(repo, num)['head']['ref']
    else:
      return num
# end function main                                                                                                                                  

##############################################                                                                                                       
# Arguments:                                                                                                                                         
#   0: this script name                                                                                                                              
#   1: repo name                                                                                                                                     
##############################################                                                                                                       
if __name__ == "__main__":
    print(main(sys.argv))
