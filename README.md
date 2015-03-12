# hegira-components

## Installation

##### Configuration

After having downloaded the source code a new file should be created under the folder src/main/resources:
* credentials.properties

###### credentials.properties
Contains the credentials that Hegira 4Clouds needs to access the databases. Currently supported databases are Google AppEngine Datastore and Microsoft Azure Tables:

```java
azure.storageConnectionString=<escaped-azure-storage-connection-string>
datastore.username=<appengine-account-email-address>
datastore.password=<appengine-account-password>
datastore.server=<application-name>.appspot.com
zookeeper.connectString=<zookeeper-ip-address>:<port>
```

##### Build
The project is Maven compliant, hence by executing the command ```mvn clean package``` the proper packages will be created.

##### Deploy
In order to interact with Google AppEngine Datastore, an application should be deployed and run on Google AppEngine; in particular, the application web.xml file should allow `Remote Api` as described [here](https://cloud.google.com/appengine/docs/java/tools/remoteapi)

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
