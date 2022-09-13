#!/bin/bash

curr_dir=$(cd `dirname $0` && pwd)
cd $curr_dir

#echo "Compiling createVMInstances.java ..."
#javac -cp $curr_dir/java-jar/lib/*:$curr_dir/java-jar/jar/*:. $curr_dir/createVMInstances.java

#echo "Compiling deleteVMInstance.java ..."
#javac -cp $curr_dir/java-jar/lib/*:$curr_dir/java-jar/jar/*:. $curr_dir/deleteVMInstance.java

#echo "Compiling getInstanceNumber.java ..."
#javac -cp $curr_dir/java-jar/lib/*:$curr_dir/java-jar/jar/*:. $curr_dir/getInstanceNumber.java

echo "Compiling changeSnapshot.java..."
javac -cp $curr_dir/java-jar/lib/*:$curr_dir/java-jar/jar/*:. $curr_dir/changeSnapshot.java

echo "Compiling poweroffVMInstance.java..."
javac -cp $curr_dir/java-jar/lib/*:$curr_dir/java-jar/jar/*:. $curr_dir/poweroffVMInstance.java

cd -
