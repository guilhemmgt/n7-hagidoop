SP='../src/'
BIN='../bin/'
cd $SP
javac -d $BIN $SP/hdfs/*.java 
javac -d $BIN $SP/application/*.java
javac -d $BIN $SP/config/*.java
javac -d $BIN $SP/daemon/*.java
javac -d $BIN $SP/interfaces/*.java
javac -d $BIN $SP/io/*.java
javac -d $BIN $SP/tests/*.java
