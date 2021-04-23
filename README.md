# elasticsearch-proxy

Tool to transparently pass Elasticsearch requests to RabbitMQ.
![elasticsearch-proxy](https://github.com/bigdotsoftware/rabbitmq-tools/raw/master/elasticsearch-proxy.png)

to run
npm install
npm start

configuration is inside server.js file

any request like:
curl -XPOST 'http://127.0.0.1:9200/sampleindex/_doc/' -H 'Content-Type: application/json' -d '{
   "hello" : "world",
   "queueName" : "q.testqueue2222a"
}'

curl -XPOST 'http://127.0.0.1:9200/sampleindex/_doc/my_id' -H 'Content-Type: application/json' -d '{
   "hello" : "world",
   "queueName" : "q.testqueue2222a"
}'

will be passed to RabbitMQ into the queue pointed by the "queueName" atttribute (see configuration inside server.js file)

# rabbitmq-iteratorage

Example of extending RabbitMQ by iterator age feature (how much time messages spend in the queue). Time is calculated between publication and successful ACK.
![rabbitmq-iteratorage](https://github.com/bigdotsoftware/rabbitmq-tools/raw/master/rabbitmq-iteratorage.png)

to run
npm install
npm start

configuration is inside server.js file