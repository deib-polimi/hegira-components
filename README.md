# hegira-components

## Installation

##### Configuration

<<<<<<< HEAD
After having downloaded the source code a new file should be created under the folder ``src/main/resources``:

* credentials.properties

In case one wants to configure hegira-components also for Apache Cassandra the file ``cassandraConfiguration.properties`` should be added to the same folder.

The structure of these files is shown below: 

###### credentials.properties
Contains the credentials that Hegira 4Clouds needs to access the databases. Currently supported databases are Google AppEngine Datastore, Microsoft Azure Tables and Apache Cassandra:

```java
azure.storageConnectionString=<escaped-azure-storage-connection-string>
datastore.username=<appengine-account-email-address>
datastore.password=<appengine-account-password>
datastore.server=<application-name>.appspot.com
zookeeper.connectString=<zookeeper-ip-address>:<port>
cassandra.server=<ip-address>
cassandra.username=<Cassandra-username>
cassandra.password=<Cassandra-password>
```
###### cassandraConfiguration.properties
This file is located in the folder src/main/resources. It contains parameters that the user has to specify if the migration is from or to Cassandra. 

```java
cassandra.keyspace=<keyspace>
cassandra.readConsistency=<consistency-type>
cassandra.primarKey=<primary-key-column-name>
```
The specified keyspace is the one in which data will be inserted.   
The consistency settings affect only read operations. The supported levels are: "eventual" and "strong".   
The primary key parameter specifies the name of the column that will be used as the primary key in all the tables to be migrated.  

##### Build
The project is Maven compliant, hence by executing the command ```mvn clean package``` the proper packages will be created.

##### Deploy
<<<<<<< HEAD
In order to interact with Google AppEngine Datastore, an application should be deployed and run on Google AppEngine; in particular, the application web.xml file should allow for `Remote Api` to be accepted by the Google AppEngine application, as described [here](https://cloud.google.com/appengine/docs/java/tools/remoteapi)

## Usage: 
Once compiled, a jar file (with dependencies) is generated inside the target/ folder.
To execute one of the components, issue the following command:
```java
java -jar hegira-components.jar [options]
```
  Options:
  
    --queue, -q
    
       RabbitMQ queue address.
       
       Default: localhost
       
    --type, -t
   
       launch a SRC or a TWC
