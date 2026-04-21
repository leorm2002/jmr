# JMR Distributed System


## Avvio manuale tramite fat jar
Una volta avviato si potrà trovare la dashboard del master al suo ip alla porta assegnata + 1000 (es 11000 nel caso sotto)
### Avvio con discovery automatico

####  Avvio dei worker

java -Djava.net.preferIPv4Stack=true -jar jmr-worker-fat.jar -p 10001 -w 1 -sd .

java -Djava.net.preferIPv4Stack=true -jar jmr-worker-fat.jar -p 10002 -w 2 -sd .

#### Avvio del master

java -Djava.net.preferIPv4Stack=true -jar .\jmr-master-fat.jar -p 10000 -sd .


### Avvio con indirizzi statici

java -Djava.net.preferIPv4Stack=true -cp .\jmr-worker-fat.jar it.jmr.worker.StaticWorkerLauncher -p 10001 -w 1 -sd storageDir

java -Djava.net.preferIPv4Stack=true -cp .\jmr-worker-fat.jar it.jmr.worker.StaticWorkerLauncher -p 10002 -w 2 -sd storageDir

#### Avvio del Master statico:

java -Djava.net.preferIPv4Stack=true -cp .\jmr-master-fat.jar it.jmr.master.StaticMasterLauncher -p 10000 -sd . --worker 1:127.0.0.1:10001 2:127.0.0.1:10002

## Esempio implementazione job wordcounting distribuito
https://github.com/leorm2002/jmr-wc

## Esempio integrazione ed esecuzione di un job in un cluster locale tramite docker
https://github.com/leorm2002/jmr-integration

## Immagini docker dei vari componenti

- [leo02n/jmr-wc](https://hub.docker.com/r/leo02n/jmr-wc)
- [leo02n/jmr-wc-failing](https://hub.docker.com/r/leo02n/jmr-wc-failing)
- [leo02n/jmr-worker](https://hub.docker.com/r/leo02n/jmr-worker)
- [leo02n/jmr-master](https://hub.docker.com/r/leo02n/jmr-master)
