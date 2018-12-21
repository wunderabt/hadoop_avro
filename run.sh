
#!/bin/bash
rm -fr output
mvn exec:java -q -Dexec.mainClass=example.MapReduceMD5sum -Dexec.args="input output"
