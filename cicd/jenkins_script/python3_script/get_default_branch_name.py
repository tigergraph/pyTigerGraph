#!/usr/bin/python3                                                                                                                                    
import util
import sys

def main(parameters):
    repo_name = parameters[1]
    return util.get_default_branch(repo_name)
# end function main                                                                                                                                  

##############################################                                                                                                       
# Arguments:                                                                                                                                         
#   0: this script name                                                                                                                              
#   1: repo name                                                                                                                                     
##############################################                                                                                                       
if __name__ == "__main__":
    print(main(sys.argv))
