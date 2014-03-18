Description
-----------

Implementation of LogAnalysis. 


Compile
-------

$ mvn clean install

Run
---
To Execute the jar file eneter this command

sudo hadoop jar LogAnalysis-0.0.1.jar com.visualpath.hadoop.loganalysis.client.ProcessLogsClient /mnt/hgfs/HadoopStuff/Data/www1 /home/cloudera/LogOutput

Here from the above command 

/mnt/hgfs/HadoopStuff/Data/www1 is the Input path

/home/cloudera/LogOutput is the OutputPath
