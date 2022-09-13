#!/usr/bin/python3                                                                                                                                    
import util
import sys, base64

def main(parameters):
    repo_name = parameters[1]
    response= util.get_file_content(repo_name, "master", "product_version")
    print(response.json())
    return base64.b64decode(response.json()['content'])
# curl -s https://<token>@raw.githubusercontent.com/tigergraph/product/master/product_version                                                        
# end function main                                                                                                                                  

##############################################                                                                                                       
# Arguments:                                                                                                                                         
#   0: this script name                                                                                                                              
#   1: repo name                                                                                                                                     
##############################################                                                                                                       
if __name__ == "__main__":
    print(main(sys.argv))
