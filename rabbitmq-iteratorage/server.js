//--------------------------------------------
// Author
// Piotr Zuk <piotr.zuk@bigdotsoftware.pl>
// www.bigdotsoftware.pl
//
//--------------------------------------------
// Disclaimer
//
// THIS PROJECT IS NOT PRODUCTION READY, USE 
// IT ON YOUR OWN RISK
//
//--------------------------------------------
// TODO
// - handle amqp_defs.BasicNack
// - handle multiple ACKs 
// - handle situation when one message is routed to more than one queue. Currently consumption from first queue closes the metric. Consumption from next queues will override the same metric in Elasticsearch
//
//--------------------------------------------

//--------------------------------------------
// Deps
const net = require('net');
const amqp_frame = require('amqplib/lib/frame');
const amqp_defs = require('amqplib/lib/defs');
const Bits = require('bitsyntax');
const elasticsearch = require("elasticsearch")
const { v4: uuidv4 } = require('uuid');

//--------------------------------------------
// Configuration
const config = {
    listen: {
        port: process.env.AMQP_INPUT_PORT || 5682
    },
    rabbitmq: {
        host: process.env.AMQP_OUTPUT_HOST || '127.0.0.1',
        port: process.env.AMQP_OUTPUT_PORT || 5672
    },
    console: {
        show: {
            incommingFrames: false,
            decodedMethod: false,
            decodedHeader: false,
            decodedBody: false,
            modifiedBuffers: false,
            tracingBuffers: false
        }
    },
    headersIngestor: {
        active: true,
        publishTimestamp: {
            name: 'pub_timestamp',
            enabled: true
        },
        publishUUID: {
            name: 'pub_uuid',
            enabled: true
        }
    },
    elasticsearch: {
        /* don't disable _source in the mapping - script call the '_update' on documents in Elasticsearch */
        active: true,
        host: 'http://127.0.0.1:9200',
        index: 'my_metrics',
        type: '_doc'
    }
};

//--------------------------------------------
// Defs
var frameHeaderPattern = Bits.matcher('type:8', 'channel:16', 'size:32', 'rest/binary');

//--------------------------------------------
// Elasticsearch
var esClient = null;
if (config.elasticsearch.active) {
    esClient = elasticsearch.Client({
        host: "http://127.0.0.1:9200",
    });
}

//--------------------------------------------
// Server
var server = net.createServer(function (socket) {

    var recvbuffer = Buffer.alloc(0);
    var sendbuffer = Buffer.alloc(0);

    var client = new net.Socket();
    client.connect(config.rabbitmq.port, config.rabbitmq.host, function () {
        console.log(`Connected to RabbitMQ ${config.rabbitmq.host}:${config.rabbitmq.port}`);
    });

    client.on('data', function (data) {
        //console.log('<==' + data.length);
        if (recvbuffer.length > 0 && data !== null)
            recvbuffer = Buffer.concat([recvbuffer, data]);
        if (recvbuffer.length == 0 && data !== null)
            recvbuffer = data;

        recvbuffer = traceIncomingBuffer(recvbuffer);

        //client.destroy(); // kill client after server's response
        socket.write(data);
    });

    client.on('close', function () {
        console.log('Connection closed');
    });

    client.on('error', function (msg) {
        console.log('Connection ERROR ' + msg);
    });

    socket.on('data', function (data) {
        //passing data to the RabbitMQ
        //var ttt = new Date().toISOString();
        //console.log(ttt + '==>' + data.length);

        var results = { frames: [] };
        if (config.headersIngestor.active) {
            if (sendbuffer.length > 0 && data !== null)
                sendbuffer = Buffer.concat([sendbuffer, data]);
            if (sendbuffer.length == 0 && data !== null)
                sendbuffer = data;

            results = traceOutgoingBuffer(sendbuffer);

            sendbuffer = results.rest;
        }        

        if (results.frames.length > 0) {
            if (config.console.show.modifiedBuffers) console.log('-------------- buffer modification BEGIN -------------------');
            results.frames.forEach(f => {
                if (config.console.show.modifiedBuffers) console.dir(f.frame, { 'maxArrayLength': null })
                client.write(f.frame);
            })
            if (config.console.show.modifiedBuffers) console.log('-------------- buffer modification ORIGINAL -------------------');
            if (config.console.show.modifiedBuffers) console.dir(data, { 'maxArrayLength': null })
            if (config.console.show.modifiedBuffers) console.log('-------------- buffer modification END -------------------');
        } else {
            client.write(data);
        }

    });

    socket.on('error', function (msg) {
        console.log('socket ERROR ' + msg);
    });
});


function closeMetric(oldts, id) {

    const ts = new Date().getTime();
    const age = ts - oldts;

    esClient.update({
        index: config.elasticsearch.index,
        type: config.elasticsearch.type,
        id: id,
        body: {
            doc: {
                "timeEnd": ts,
                "age": age
            }
        }
    })
    .then(response => {
        console.log('Metric updated');
    })
    .catch(err => {
        console.log('ERROR Metric not updated: ' + err);
    })
}

function createMetric(exchange, routingKey, ts, id) {

    esClient.index({
        index: config.elasticsearch.index,
        type: config.elasticsearch.type,
        id: id,
        body: {
            "@timestamp": new Date(ts).toISOString(),
            "exchange": exchange,
            "routingKey": routingKey,
            "timeStart": ts,
            "timeEnd": null,
            "age": null
        }
    })
    .then(response => {
        console.log('Metric indexed');
    })
    .catch(err => {
        console.log('ERROR Metric not indexed: ' + err);
    })
}

function traceIncomingBuffer(buffer) {
    var recvbuffer = buffer;
    var i = 0;
    
    while (true) {
        if (recvbuffer.length == 0)
            break;

        //console.log(`------------ INCOMING BUFFER index=(${i}) BEGIN -----------`);
        //console.dir(recvbuffer, { 'maxArrayLength': null })
        //console.log(`------------ INCOMING BUFFER index=(${i}) END -----------`);

        var frameobj = amqp_frame.parseFrame(recvbuffer, amqp_defs.constants.FRAME_MIN_SIZE);

        if (!frameobj) {
            console.log('ERROR cannot decode data incoming from RabbitMQ');
            break;
        }
        else
        {
            recvbuffer = frameobj.rest;
            var objdecoded = amqp_frame.decodeFrame(frameobj);
            if (config.console.show.incommingFrames)
                console.log(objdecoded);

            if (config.elasticsearch.active) {
                switch (objdecoded.id) {
                    case amqp_defs.BasicProperties:
                        if (objdecoded.fields.headers) {
                            const msg_uuid = objdecoded.fields.headers[config.headersIngestor.publishUUID.name];
                            const msg_ts = objdecoded.fields.headers[config.headersIngestor.publishTimestamp.name];
                            if (msg_uuid != null && msg_ts != null) {
                                console.log(`Closing metric ${msg_uuid}`);
                                closeMetric(msg_ts, msg_uuid);
                            }
                        }
                        break;
                }
            }

        }
        i++;
    }

    return recvbuffer;
}

function traceOutgoingBuffer(buffer) {

    var sendbuffer = buffer;
    var extractedFrames = [];
    var previousFrameMethodInfo = null;
    var i = 0;
    while (true) {
        if (sendbuffer.length == 0)
            break;

        var fh = frameHeaderPattern(sendbuffer);

        if (config.console.show.tracingBuffers) {
            console.log(`------------ TRACING BUFFER index=(${i}) BEGIN -----------`);
            console.dir(sendbuffer, { 'maxArrayLength': null })
            console.log(`------------ TRACING BUFFER index=(${i}) END -----------`);
            //console.log(fh);
        }
        if (fh) {
            var offset = 1 + 2 /* channel*/ + 4 /*size*/ + fh.size /*buffer*/ + 1 /*FRAME_END*/;
            switch (fh.type) {
                case amqp_defs.constants.FRAME_HEARTBEAT:   /*8*/

                    if (fh.rest.length <= fh.size || fh.rest[fh.size] !== amqp_defs.constants.FRAME_END)
                        break;

                    previousFrameMethodInfo = null;
                    extractedFrames.push({ index: i, frame: sendbuffer.subarray(0, offset) });
                    sendbuffer = sendbuffer.slice(offset); //remove first N bytes from sendbuffer
                    break;

                case amqp_defs.constants.FRAME_METHOD:      /*1*/
                
                    if (fh.rest.length <= fh.size || fh.rest[fh.size] !== amqp_defs.constants.FRAME_END)
                        break;

                    //need to extract exchange and routing key from it
                    var frameobj = amqp_frame.parseFrame(sendbuffer, amqp_defs.constants.FRAME_MIN_SIZE);
                    if (frameobj) {
                        //console.log(frameobj);
                        var decodedFrame = amqp_frame.decodeFrame(frameobj);
                        if (config.console.show.decodedMethod) {
                            console.log(`Decoded FRAME_METHOD (rest len: ${frameobj.rest.length})`);
                            console.log(decodedFrame);
                        }

                        if (decodedFrame.id == amqp_defs.BasicPublish) {
                            previousFrameMethodInfo = {
                                id: decodedFrame.id,
                                exchange: decodedFrame.fields.exchange,
                                routingKey: decodedFrame.fields.routingKey
                            };
                        }
                        if (decodedFrame.id == amqp_defs.BasicConsume) {
                            previousFrameMethodInfo = {
                                id: decodedFrame.id,
                                queue: decodedFrame.fields.queue
                            };
                        }

                        //TODO
                        // - handle amqp_defs.BasicNack
                        // - handle multiple ACKs 
                        if (decodedFrame.id == amqp_defs.BasicAck) {    
                            id: decodedFrame.id
                        }
                        

                    }

                    extractedFrames.push({ index: i, frame: sendbuffer.subarray(0, offset) });
                    sendbuffer = sendbuffer.slice(offset); //remove first N bytes from sendbuffer
                    break;

                case amqp_defs.constants.FRAME_HEADER:      /*2*/

                    if (fh.rest.length <= fh.size || fh.rest[fh.size] !== amqp_defs.constants.FRAME_END)
                        break;

                    var frameobj = amqp_frame.parseFrame(sendbuffer, amqp_defs.constants.FRAME_MIN_SIZE);
                    if (frameobj) {
                        //console.log(frameobj);
                        var decodedFrame = amqp_frame.decodeFrame(frameobj);
                        if (config.console.show.decodedHeader) {
                            console.log(`Decoded FRAME_HEADER (rest len: ${frameobj.rest.length})`);
                            console.log(decodedFrame);
                        }

                        //add extra header with publication timestamp (if feature enabled)
                        const ts = new Date().getTime();
                        const uniqueMessageID = ts + '_' + uuidv4().replace('-', '').substr(0, 13);

                        if (config.headersIngestor.publishTimestamp.enabled) {
                            if (!decodedFrame.fields.headers) decodedFrame.fields.headers = {};
                            decodedFrame.fields.headers[config.headersIngestor.publishTimestamp.name] = ts;
                        }
                        if (config.headersIngestor.publishUUID.enabled) {
                            if (!decodedFrame.fields.headers) decodedFrame.fields.headers = {};
                            decodedFrame.fields.headers[config.headersIngestor.publishUUID.name] = uniqueMessageID;
                        }

                        if (config.elasticsearch.active && previousFrameMethodInfo != null) {
                            switch (previousFrameMethodInfo.id) {
                                case amqp_defs.BasicPublish:
                                    createMetric(previousFrameMethodInfo.exchange, previousFrameMethodInfo.routingKey, ts, uniqueMessageID);
                                    break;
                            }
                        }

                        var pframe = amqp_defs.encodeProperties(
                            decodedFrame.id,
                            decodedFrame.channel,
                            decodedFrame.size,  /* body remains unchanged, so the same size goes here */
                            decodedFrame.fields);

                        extractedFrames.push({ index: i, frame: pframe });
                    } else {
                        extractedFrames.push({ index: i, frame: sendbuffer.subarray(0, offset) });
                    }

                    sendbuffer = sendbuffer.slice(offset); //remove first N bytes from sendbuffer
                    break;

                case amqp_defs.constants.FRAME_BODY:        /*3*/

                    if (fh.rest.length <= fh.size || fh.rest[fh.size] !== amqp_defs.constants.FRAME_END)
                        break;

                    if (config.console.show.decodedBody) {
                        var frameobj = amqp_frame.parseFrame(sendbuffer, amqp_defs.constants.FRAME_MIN_SIZE);
                        if (frameobj) {
                            //sendbuffer = frameobj.rest;
                            extractedFrames.push({ index: i, frame: sendbuffer.subarray(0, offset) });
                            sendbuffer = sendbuffer.slice(offset); //remove first N bytes from sendbuffer
                            var objdecoded = amqp_frame.decodeFrame(frameobj);
                            console.log(`Decoded FRAME_BODY (rest len: ${frameobj.rest.length})`);
                            console.log(objdecoded);
                        } else {
                            console.log('ERROR - cannot parse buffer');
                            sendbuffer = Buffer.alloc(0);
                        }
                    } else {
                        extractedFrames.push({ index: i, frame: sendbuffer.subarray(0, offset) });
                        sendbuffer = sendbuffer.slice(offset); //remove first N bytes from sendbuffer
                    }
                    previousFrameMethodInfo = null;
                    break;

                case 65:    //AMQP0091 PROTOCOL_HEADER
                    sendbuffer = Buffer.alloc(0);
                    previousFrameMethodInfo = null;
                    break;

                default:
                    sendbuffer = Buffer.alloc(0);
                    previousFrameMethodInfo = null;
                    break;
            }
        } else {
            console.log('ERROR - cannot parse buffer');
            sendbuffer = Buffer.alloc(0);
        }
        i++;
    }

    return { rest: sendbuffer, frames: extractedFrames };
}



console.log("Waiting for incomming conenction on port " + config.listen.port);

server.listen(config.listen.port);