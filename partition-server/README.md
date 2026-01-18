# Distributed E-Wallet System

Project Structure

* partition-server – Runs partition replicas (leader + followers)
* ewallet-client – Client applications (Clerk & Regular Client)
* partition-resolver – Resolves partitions for accounts
* lock-service – Distributed locking using ZooKeeper
* name-service – Service discovery using etcd


Prerequisites

* Java 8 or later
* Maven
* ZooKeeper 3.6.2
* etcd v3.4.15
* Linux / macOS terminal (or WSL for Windows)

Build Instructions

Each package must be built separately before running the system.
From each package directory, run:

```bash
mvn clean install
```


Running the System

Step 1: Start ZooKeeper

cd apache-zookeeper-3.6.2-bin
./bin/zkServer.sh start conf/zoo_sample.cfg



Step 2: Start etcd

cd etcd-v3.4.15
./etcd



Step 3: Start Partition Servers

Navigate to the server project:
cd Distributed\ E-Wallet\ System/partition-server

Partition 0:
java -jar target/partition-server-1.0-SNAPSHOT-jar-with-dependencies.jar 11000 0
java -jar target/partition-server-1.0-SNAPSHOT-jar-with-dependencies.jar 11002 0
java -jar target/partition-server-1.0-SNAPSHOT-jar-with-dependencies.jar 11004 0

Partition 1:
java -jar target/partition-server-1.0-SNAPSHOT-jar-with-dependencies.jar 11001 1
java -jar target/partition-server-1.0-SNAPSHOT-jar-with-dependencies.jar 11003 1
java -jar target/partition-server-1.0-SNAPSHOT-jar-with-dependencies.jar 11005 1



Step 4: Start Clients

Navigate to the client project:
cd Distributed\ E-Wallet\ System/ewallet-client

Clerk Client:
java -jar target/ewallet-client-1.0-SNAPSHOT-jar-with-dependencies.jar clerk

Regular Client:
java -jar target/ewallet-client-1.0-SNAPSHOT-jar-with-dependencies.jar client