//--------------------------------------------
// Disclaimer
//
// THIS PROJECT IS NOT PRODUCTION READY, USE 
// IT ON YOUR OWN RISK
//--------------------------------------------

//--------------------------------------------
// Deps
const express = require('express')
const bodyParser = require('body-parser');
const throttle = require("express-throttle");
const amqp = require('amqplib');
const EventEmitter = require('events');

//--------------------------------------------
// Configuration
const config = {
    port: 9200,
    extractQueueFn: (json) => {
        return json.queueName;
    },
    rabbitMQ: {
        user: '',
        pass: '',
        host: process.env.AMQP_URL || 'amqp://localhost:5672'
    }
};

//--------------------------------------------
// Emitter
class RabbitMQEmitter extends EventEmitter { }
const rabbitEmitter = new RabbitMQEmitter();

//--------------------------------------------
// RabbitMQ connection
var rabbitConnection = {
    connection: null,
    channel: null,
    eventEmitterRegistered: false
};

async function rabbitConnect() {

    if (!rabbitConnection.connection) {
        console.log('> Initialize RabbitMQ connection');
        rabbitConnection.connection = await amqp.connect(config.rabbitMQ.host);
        rabbitConnection.connection.on('error', (err) => {
            console.log('ERROR - connection error: ' + err);
            rabbitConnection.connection = null;
        });
        console.log('> Initialize RabbitMQ connection - CONNECTED');
        rabbitConnection.channel == null;
    }

    if (!rabbitConnection.channel) {
        rabbitConnection.channel = await rabbitConnection.connection.createChannel();
        rabbitConnection.channel.on('error', (err) => {
            console.log('ERROR - channel error: ' + err);
            rabbitConnection.channel = null;
        });
        console.log('> Initialize RabbitMQ connection - CHANNEL CREATED');
    }

    if (rabbitConnection.eventEmitterRegistered)
        return;

    rabbitEmitter.on('event', (queue, index, type, id, body, res) => {

        rabbitConnection.eventEmitterRegistered = true;
        rabbitConnection.channel.assertQueue(queue, {
            durable: true
        });

        let result = rabbitConnection.channel.sendToQueue(
            queue, 
            Buffer.from(JSON.stringify(body)), 
            { headers: {
               _index: index,
               _type: type,
               _id: id
            }}
        );
        if (result) {
            console.log("> Sent to the queue %s", queue);
            res.status(200).send({
                _index: index,
                _type: type,
                _id: id,                    /*we know id only when provided in the request, otherwise - null */
                _version: null,             /*we have no version info */
                result: null                /*"created", "updated" - we don't know the result */,
                _shards: { "total": null, "successful": null, "failed": null },     /*we have no stats info */
                _seq_no: null,              /*we have no sequence info */
                _primary_term: null         /*we have no primary term info */
            });
        } else {
            console.log("> ERROR - An error has occurred while sending message to the queue %s", queue);
            reply400('An error has occurred while sending message to the queue', res);
        }

    });
}

//--------------------------------------------
// Express
const app = express();
app.use(bodyParser.json({ limit: '3mb' }));
app.use(function (error, req, res, next) {
    console.log('> ERROR - cannot parse the request body');
    reply400(JSON.stringify(error), res);
});

function reply400(error, res) {
    res.status(400).send({
        error: {
            root_cause: [{
                type: "mapper_parsing_exception",
                reason: "failed to parse"
            }],
            type: "mapper_parsing_exception",
            reason: "failed to parse",
            caused_by: {
                type: "json_parse_exception",
                reason: error
            }
        }, status: 400
    });
}

var throttle_options = {
    "rate": "5/s",
    //"burst": 5,
    //"period": "1s",
    "cost": function (req) {
        return 1;
    },
    "on_allowed": function (req, res, next, bucket) {
        res.set("X-Rate-Limit-Limit", 5);
        res.set("X-Rate-Limit-Remaining", bucket.tokens);
        var ip_address = req.connection.remoteAddress;
        console.log('> IP ' + ip_address + ' allowed, remianing tokens:' + bucket.tokens);
        next();
    },
    "on_throttled": function (req, res, next, bucket) {
        var ip_address = req.connection.remoteAddress;
        console.log('> IP ' + ip_address + ' throttled!');
        // Possible course of actions: 
        // 1) Log request 
        // 2) Add client ip address to a ban list 
        // 3) Send back more information 
        console.log(req.method + ' ' + req.originalUrl);
        res.set("X-Rate-Limit-Limit", 5);
        res.set("X-Rate-Limit-Remaining", 0);
        // bucket.etime = expiration time in Unix epoch ms, only available 
        // for fixed time windows 
        res.set("X-Rate-Limit-Reset", bucket.etime);
        res.status(429).send();
    }
};

app.post('/:index/:type', throttle(throttle_options), (req, res) => {
    processRequest(req.params.index, req.params.type, null, req.body, res);
});

app.post('/:index/:type/:id', throttle(throttle_options), (req, res) => {
    processRequest(req.params.index, req.params.type, req.params.id, req.body, res);
});

async function processRequest(index, type, id, body, res) {
    res.header('Access-Control-Allow-Origin', '*');
    const queue = config.extractQueueFn(body);
    if (queue != null) {
        await rabbitConnect()
            .then(() => {
                rabbitEmitter.emit('event', queue, index, type, id, body, res);
            })
            .catch((err) => {
                rabbitConnection.connection = null;
                rabbitConnection.channel = null;
                console.log('> Initialize RabbitMQ connection - ERROR ' + err);
                reply400(JSON.stringify(err), res);
            })
        
    } else {
        console.log('> ERROR - cannot extract queue name from the request body');
        reply400("Cannot extract queue name from the request body", res);
    }
}

app.listen(config.port, () => console.log('> elasticsearch-proxy listening on port ' + config.port));

