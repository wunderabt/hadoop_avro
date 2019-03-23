
#!/bin/bash
rm -fr output
mvn exec:java -q -Dexec.mainClass=poc.MapReduceMD5sum -Dexec.args="input output"
