export USER=ubuntu

sudo apt-get update && sudo apt-get -y upgrade
sudo apt-get -y install openjdk-7-jdk tomcat6 mvn git-core
echo "deb http://www.rabbitmq.com/debian/ testing main" | sudo tee --append /etc/apt/sources.list
echo "deb http://packages.erlang-solutions.com/debian precise contrib" | sudo tee --append /etc/apt/sources.list
wget http://packages.erlang-solutions.com/ubuntu/erlang_solutions.asc
sudo apt-key add erlang_solutions.asc
rm erlang_solutions.asc
wget http://www.rabbitmq.com/rabbitmq-signing-key-public.asc
sudo apt-key add rabbitmq-signing-key-public.asc
rm rabbitmq-signing-key-public.asc

sudo apt-get update && sudo apt-get -y install rabbitmq-server
sudo apt-get -f -y install

#allowing external UNAUTHORIZED access
echo "[{rabbit, [{loopback_users, []}]}]." | sudo tee --append /etc/rabbitmq/rabbitmq.config

sudo rm -r /usr/share/tomcat6/webapps
sudo ln -s /var/lib/tomcat6/conf /usr/share/tomcat6/conf
sudo chown -R $USER:$USER /var/lib/tomcat6/webapps
sudo ln -s /var/lib/tomcat6/webapps /usr/share/tomcat6/webapps
echo "JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-amd64" | sudo tee --append /etc/default/tomcat6

cd ~
git clone https://github.com/deib-polimi/hegira-api.git
git clone https://github.com/deib-polimi/hegira-components.git
cd hegira-api
mvn clean package -DskipTests
cp target/hegira-api.war /usr/share/tomcat6/webapps
