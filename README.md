# JMR Distributed System

## Immagini docker

- [leo02n/jmr-wc](https://hub.docker.com/r/leo02n/jmr-wc)
- [leo02n/jmr-wc-failing](https://hub.docker.com/r/leo02n/jmr-wc-failing)
- [leo02n/jmr-worker](https://hub.docker.com/r/leo02n/jmr-worker)
- [leo02n/jmr-master](https://hub.docker.com/r/leo02n/jmr-master)

## Avvio manuale tramite fat jar

### Avvio con discovery automatico

####  Avvio dei worker

java -Djava.net.preferIPv4Stack=true -jar jmr-worker-fat.jar -p 1001 -w 1 -sd .

java -Djava.net.preferIPv4Stack=true -jar jmr-worker-fat.jar -p 1002 -w 2 -sd .

#### Avvio del master

java -Djava.net.preferIPv4Stack=true -jar .\jmr-master-fat.jar -p 1000 -sd .


### Avvio con indirizzi statici

java -Djava.net.preferIPv4Stack=true -cp .\jmr-worker-fat.jar it.jmr.worker.StaticWorkerLauncher -p 10001 -w 1 -sd storageDir

java -Djava.net.preferIPv4Stack=true -cp .\jmr-worker-fat.jar it.jmr.worker.StaticWorkerLauncher -p 10002 -w 2 -sd storageDir

#### Avvio del Master statico:

java -Djava.net.preferIPv4Stack=true -cp .\jmr-master-fat.jar it.jmr.master.StaticMasterLauncher -p 10000 -sd . --worker 1:127.0.0.1:10001 2:127.0.0.1:10002

## job di esempio
https://github.com/leorm2002/jmr-wc

## repo con esempio di integrazione
https://github.com/leorm2002/jmr-integration