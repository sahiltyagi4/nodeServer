const express = require('express')  
const app = express()  
const port = 3000

app.get('/', (request, response) => {  
  response.send('listening on port 3000!')
})

var kafka = require('kafka-node'),
     Producer = kafka.Producer,
     client = new kafka.Client('localhost:2181'),
     producer = new Producer(client);


var testJson = require('./test2.json');

// console.log(testJson);

var avroSchemaJson = require('./avroSchema.json');


var avro = require('avsc');

// Parse the schema.
var logType = avro.parse(avroSchemaJson);

// // And its corresponding Avro encoding.
var buf = logType.toBuffer(testJson);



payloads = [
         { topic: 'test', messages: buf, partition: 0 },
     ];

 // producer 'on' ready to send payload to kafka.
 producer.on('ready', function(){
     producer.send(payloads, function(err, data){
         console.log(data)
     });
 });

 producer.on('error', function(err){
 	console.log(err)
 })     

app.listen(port, (err) => {  
  if (err) {
    return console.log('something bad happened', err)
  }

  console.log(`server is listening on ${port}`)
})