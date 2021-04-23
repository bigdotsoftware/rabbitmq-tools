//--------------------------------------------
// Disclaimer
//
// THIS PROJECT IS NOT PRODUCTION READY, USE 
// IT ON YOUR OWN RISK
//--------------------------------------------

//--------------------------------------------
// Deps
const net = require('net');
const amqp_frame = require('amqplib/lib/frame');
const amqp_defs = require('amqplib/lib/defs');
const Bits = require('bitsyntax');

//--------------------------------------------
// Configuration
const config = {
    input: {
        port: process.env.AMQP_INPUT_PORT || 5682
    },
    output: {
        host: process.env.AMQP_OUTPUT_HOST || '127.0.0.1',
        port: process.env.AMQP_OUTPUT_PORT || 5672
    },
    console: {
        show: {
            incommingFrames: false,
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
        }
    }
};

//--------------------------------------------
// Defs
var frameHeaderPattern = Bits.matcher('type:8', 'channel:16', 'size:32', 'rest/binary');

//--------------------------------------------
// Server
var server = net.createServer(function (socket) {

    var recvbuffer = Buffer.alloc(0);
    var sendbuffer = Buffer.alloc(0);

    var client = new net.Socket();
    client.connect(config.output.port, config.output.host, function () {
        console.log(`Connected to RabbitMQ ${config.output.host}:${config.output.port}`);
    });

    client.on('data', function (data) {
        //console.log('<==' + data.length);
        if (recvbuffer.length > 0 && data !== null)
            recvbuffer = Buffer.concat([recvbuffer, data]);
        if (recvbuffer.length == 0 && data !== null)
            recvbuffer = data;

        var frameobj = amqp_frame.parseFrame(recvbuffer, amqp_defs.constants.FRAME_MIN_SIZE);

        if (!frameobj) {
            console.log('ERROR cannot decode data incoming from RabbitMQ');
        }
        else {
            recvbuffer = frameobj.rest;
            var objdecoded = amqp_frame.decodeFrame(frameobj);
            if (config.console.show.incommingFrames)
                console.log(objdecoded);
        }

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

            results = traceBuffer(sendbuffer);

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

function traceBuffer(buffer) {

    var sendbuffer = buffer;
    var extractedFrames = [];
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
                case amqp_defs.constants.FRAME_METHOD:      /*1*/
                case amqp_defs.constants.FRAME_HEARTBEAT:   /*8*/
                    if (fh.rest.length <= fh.size || fh.rest[fh.size] !== amqp_defs.constants.FRAME_END)
                        break;

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
                        if (config.headersIngestor.publishTimestamp.enabled) {
                            if (!decodedFrame.fields.headers) decodedFrame.fields.headers = {};
                            decodedFrame.fields.headers[config.headersIngestor.publishTimestamp.name] = new Date().getTime();
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
                    break;

                case 65:    //AMQP0091 PROTOCOL_HEADER
                    sendbuffer = Buffer.alloc(0);
                    break;

                default:
                    sendbuffer = Buffer.alloc(0);
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

console.log("Waiting for incomming conenction on port " + config.input.port);

server.listen(config.input.port);