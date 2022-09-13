#!/bin/bash
curr_dir=$(cd `dirname $0` && pwd)
javac -cp $curr_dir/java-jar/lib/*:$curr_dir/java-jar/jar/*:. Initiate_server.java 
java -cp $curr_dir/java-jar/lib/*:$curr_dir/java-jar/jar/*:. Initiate_server
rm -f $curr_dir/*.class
