const express = require('express');
const { Kafka } = require('kafkajs');
var WebSocketServer = require('websocket').server;
var http = require('http');

(async () => {
  console.log('Initializing kafka...');
  const kafka = new Kafka({
    clientId: 'kafka-nodejs-starter',
    brokers: ['localhost:9092'],
  });

  // Initialize the Kafka producer and consumer
  const producer = kafka.producer();
  const consumer = kafka.consumer({ groupId: 'demoTopic-consumerGroup' });

  await producer.connect();
  console.log('Connected to producer.');

  await consumer.connect();
  console.log('Connected to consumer.');

  await consumer.subscribe({ topic: 'demoTopics', fromBeginning: true });
  console.log('Consumer subscribed to topic = demoTopics');

 

  // Send an event to the demoTopic topic
  // await producer.send({
  //   topic: 'demoTopic',
  //   messages: [{ value: 'This event came from another service.' }],
  // });
  // console.log('Produced a message.');

  // Disconnect the producer once we're done
  // await producer.disconnect();

  // const app = express();

  // const PORT = process.env.PORT || 3001;
  // app.listen(PORT, () => {
  //   console.log(`🎉🎉🎉 Application running on port: ${PORT} 🎉🎉🎉`);
  // });

  var server = http.createServer(function (request, response) {
    console.log(' Request recieved : ' + request.url);
    response.writeHead(404);
    response.end();
  });
  server.listen(8082, function () {
    console.log('Listening on port : 8082');
  });

  webSocketServer = new WebSocketServer({
    httpServer: server,
    autoAcceptConnections: false,
  });

  function iSOriginAllowed(origin) {
    return true;
  }

  var connection;

  webSocketServer.on('request', function (request) {
    if (!iSOriginAllowed(request.origin)) {
      request.reject();
      console.log(' Connection from : ' + request.origin + ' rejected.');
      return;
    }

    connection = request.accept('echo-protocol', request.origin);
    console.log(connection)
    console.log(' Connection accepted : ' + request.origin);
    connection.on('message', async function (message) {
      console.log("message -->> ", message.type, message)
      if (message.type === 'utf8') {
        console.log('Received Message: ' + message.utf8Data);
        await producer.send({
          topic: 'demoTopics',
          messages: [
            { value: message.utf8Data },
          ],
        });
      }
    });
    connection.on('close', function (reasonCode, description) {
      console.log('Connection ' + connection.remoteAddress + ' disconnected.');
    });
  });

 // Log every message consumed
 await consumer.run({
  eachMessage: async ({ topic, partition, message }) => {
    console.log('Consumed a message = ', {
      topic,
      partition,
      value: message.value.toString(),
    });
    try {
      if(connection)
      connection.sendUTF(message.value.toString())
    }
    catch(e){
      console.log("-->> error ", e);
    }
  },
});


})();

///Users/saisumanthavara/Desktop/legal-kafka/kafka_2.12-3.4.1

