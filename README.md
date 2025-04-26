This coursework tries to develop a Java application that reads patient data from a file in CSV
format, which is saved in Hadoop Distributed File System (HDFS). The application then
classifies the data based on specified fields (Patient ID, Region, and Symptom), generates
classified data, and writes the output files in JSON format to HDFS again with a Yarn cluster.


To compile your code successfully, the javac command needs a classpath that includes:

The current directory (.) where your .java files are located.
Your json-20250107.jar file.
The Hadoop library JAR files (hadoop-common, hadoop-hdfs, and their dependencies).
Crucially, the standard Java 11 library modules.
The standard Java 11 library is typically located in the jmods directory within your JDK installation ($JAVA_HOME/jmods). We can use the --module-path option with javac to tell it where to find these modules.


Run the javac command with the complete classpath and module path:

javac --module-path /path/to/your/java11/jmods -cp ".:json-20250107.jar:/usr/local/hadoop/share/hadoop/common/*:/usr/local/hadoop/share/hadoop/common/lib/*:/usr/local/hadoop/share/hadoop/hdfs/*:/usr/local/hadoop/share/hadoop/hdfs/lib/*" Patient.java PatientDataProcessor.java