# JMR Distributed System

## Docker Images

- [leo02n/jmr-wc](https://hub.docker.com/r/leo02n/jmr-wc)
- [leo02n/jmr-wc-failing](https://hub.docker.com/r/leo02n/jmr-wc-failing)
- [leo02n/jmr-worker](https://hub.docker.com/r/leo02n/jmr-worker)
- [leo02n/jmr-master](https://hub.docker.com/r/leo02n/jmr-master)

## Fat jars

Start a worker 
java -Djava.net.preferIPv4Stack=true -jar jmr-worker-fat.jar -p 1001 -w 1 -sd storageDir
java -Djava.net.preferIPv4Stack=true -jar jmr-worker-fat.jar -p 1002 -w 2 -sd storageDir
.....

Start the master
java "-Djava.net.preferIPv4Stack=true" -jar .\jmr-master-fat.jar -p 1000 -sd .

This will launch with auto discovery



  java -Djava.net.preferIPv4Stack=true -cp .\jmr-worker-fat.jar it.jmr.worker.StaticWorkerLauncher -p 10001 -w 1 -sd storageDir
  java -Djava.net.preferIPv4Stack=true -cp .\jmr-worker-fat.jar it.jmr.worker.StaticWorkerLauncher -p 10002 -w 2 -sd storageDir

  Master statico con piu worker:

  java -Djava.net.preferIPv4Stack=true -cp .\jmr-master-fat.jar it.jmr.master.StaticMasterLauncher -p 10000 -sd . --worker 1:127.0.0.1:10001 2:127.0.0.1:10002