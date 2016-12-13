// some basic required libs
var log4js = require('log4js');
var zlib = require('zlib');
var cluster = require('cluster');
var request = require('request');
var shortId = require('shortid');
// get config from json file
confDict = require('../conf/production_conf.json');
var listenerPort = parseInt(confDict["node_port"]);
// serialize into avro format
var avro = require('avsc');
// read the schema from the json file in the conf directory
var iotSchema = require('../conf/iot_data_schema.json');
// Parse the schema.
var iotLogType = avro.parse(iotSchema);
// import kafka libs
var kafka = require('kafka-node');
var Producer = kafka.Producer;
var kafkaConnHost = confDict["kafka_cluster_node_host"] + ":" + confDict["zk_node_conn_port"];
var kafkaTopic = confDict["kafka_iot_main_topic"];
// configure log4js
log4js.configure({
    appenders: [
        {
            type: "dateFile",
            filename: confDict["node_logs"],
            category: [ 'iot','console', 'raw' ],
        pattern: "-yyyy-MM-dd"
        },
        {
            type: "console"
        }
    ],
    replaceConsole: true
});
//setup loggers
var logger = log4js.getLogger('iot');
if(cluster.isMaster) {
    // We'll have 3 workers listening for now
    var workerCount = 3;//require('os').cpus().length;
    function messageHandler(msg) {
    }
    // Create a worker for each CPU
    logger.info("Forking "+workerCount+ " workers")
    for (var i = 0; i < workerCount; i += 1) {
        cluster.fork();
    }
    Object.keys(cluster.workers).forEach(function(id) {
        cluster.workers[id].on('message', messageHandler);
    });
    // Listen for dying workers
    cluster.on('exit', function (worker) {
        // Replace the dead worker, we're not sentimenta
        logger.error('worker dead-->' + worker.id);
        cluster.fork();
    });

}
else {
    // Listeners
    logger.info('listening on worker-->' + cluster.worker.id);
    // express is used as the webserver!
    var express = require('express'); 
    var app = express();

    // Listen to data at the given port! We'll then store the
    // data as raw logs as well as send it to kafka for 
    // further processing!
    app.post('/store/', function (req, res) {
        logger.info("######## NEW DATA PACKET RECEIVED ########");
        process_json(req);
        res.writeHead(200, "OK", {'Content-Type': 'text/plain'});
        res.end();
        // notify master about the request
        process.send({ cmd: 'notifyRequest' });
        logger.info("######## NEW DATA PACKET PROCESSING ENDED ########")
    });
    app.listen(listenerPort);

    // listen for data, apply the avro schema to it and send it to kafka!
    function process_json(req){
        var ip = req.headers['x-forwarded-for'].split(", ")[0] || 
            req.connection.remoteAddress || 
            req.socket.remoteAddress ||
            req.connection.socket.remoteAddress;
        var serverTimestamp = ((new Date).getTime()/1000).toPrecision(10).toString();
            logger.info("ip --> " + ip);
            logger.info("header info --> " + require('util').inspect(req.headers));
            logger.info("lib --> " + req.headers['x-lib']);
            logger.info("version --> " + req.headers['x-lib-ver']);
            logger.info("server timestamp --> " + serverTimestamp);
            var zlGunzip = zlib.createGunzip();
            var jsonStr = ""
            zlGunzip.on('data', function (data){
                jsonStr += data.toString();
            })
            zlGunzip.on('end', function(){
                logger.info("completed the unzip process");
                srlz_store_data(jsonStr, ip, serverTimestamp);
            });
            zlGunzip.on('error', function (err) {
                logger.info("Unzip failed!");
                logger.error(err);
            });
            req.pipe(zlGunzip);
    }

    function srlz_store_data(jsonStr, ip, serverTimestamp) {
        var json = {}
        try {
           json = JSON.parse(jsonStr);
        }
        catch (e) {
           json = {}
        }
        // add some headers that are needed
        json['server_ip'] = ip;
        json['server_time'] = serverTimestamp;
        json['packet_id'] = shortId.generate().toString();
        logger.info(JSON.stringify(json));
        // serialize json to avro format
        var iotBuf = iotLogType.toBuffer(json);
        // setup kafka connection
        logger.info("kafka-->" + kafkaConnHost + ", topic-->" + confDict["kafka_iot_main_topic"]);
        // set up kafka producer and its payload
        var client = new kafka.Client(kafkaConnHost);
        var producer = new Producer(client);
        payloads = [
         { topic: kafkaTopic, messages: iotBuf, partition: 0 },
        ];
        // producer 'on' ready to send payload to kafka.
         producer.on('ready', function(){
             producer.send(payloads, function(err, data){
                console.log(data)
                logger.info("data sent --> " + data);
             });
         });
         // kafka error
         producer.on('error', function(err){
            console.log(err)
            logger.info("######## ERROR SENDING DATA TO KAFKA ########")
            logger.info(err)
         })
        logger.info("saved raw json succesfully and sent it to kafka!")
    }
}
