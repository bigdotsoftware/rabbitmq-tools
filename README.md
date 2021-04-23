# elasticsearch-proxy

Tool to transparently pass Elasticsearch requests to RabbitMQ.
![elasticsearch-proxy](https://github.com/bigdotsoftware/rabbitmq-tools/raw/master/elasticsearch-proxy.png)

Run project

    npm install
    npm start

Configuration is inside server.js file

Sample requests:

    curl -XPOST 'http://127.0.0.1:9200/sampleindex/_doc/' -H 'Content-Type: application/json' -d '{
       "hello" : "world",
       "queueName" : "q.testqueue"
    }'

    curl -XPOST 'http://127.0.0.1:9200/sampleindex/_doc/my_id' -H 'Content-Type: application/json' -d '{
       "hello" : "world",
       "queueName" : "q.testqueue"
    }'

Body of both requests will be passed into RabbitMQ queue. Queue is specified by the "queueName" atttribute (see configuration inside server.js file)

# rabbitmq-iteratorage

Example of extending RabbitMQ by iterator age feature (how much time messages spend in the queue). Time is calculated between publication and successful ACK.
![rabbitmq-iteratorage](https://github.com/bigdotsoftware/rabbitmq-tools/raw/master/rabbitmq-iteratorage.png)

Run project

    npm install
    npm start

Configuration is inside server.js file

Sample Result

![rabbitmq-iteratorage-elasticsearch-hits](https://github.com/bigdotsoftware/rabbitmq-tools/raw/master/rabbitmq-iteratorage-elasticsearch-hits.png)

