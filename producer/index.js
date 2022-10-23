//import Kafka from 'node-rdkafka';
const Kafka = require('node-rdkafka');

console.log("Producer starts");

const stream = Kafka.Producer.createWriteStream({
    'metadata.broker.list':'localhost:9092'
}, {}, {topic: 'task'});

function randomizeIntegerBetween(from, to) {
    return (Math.floor(Math.random() * (to-from+1))) +from;
}

function queueMessage() {
    const no1 = randomizeIntegerBetween(1,2);
    const no2 = randomizeIntegerBetween(1,2);
    const no3 = randomizeIntegerBetween(2,3);

    const success = stream.write(Buffer.from(
        `Is this correct ${no1} + ${no2} = ${no3}?`
    ));
/*
    if(success) {
        console.log('Message successfully to stream');
    } else {
        console.log('Problem writing to stream');
    }*/
}

setInterval(() => {
    queueMessage();
}, 2500);

const consumer = Kafka.KafkaConsumer({
    'group.id':'kafka',
    'metadata.broker.list':'localhost:9092'
}, {});

consumer.connect();

consumer.on('ready', () => {
    console.log('Consumer ready');
    consumer.subscribe(['answer']);
    consumer.consume();
}).on('data', (data) => {
    console.log((data.value).toString());
});