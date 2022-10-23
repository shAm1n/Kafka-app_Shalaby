//import Kafka from 'node-rdkafka';
const Kafka = require('node-rdkafka');

console.log('Consumer starts');

const stream = Kafka.Producer.createWriteStream({
    'metadata.broker.list':'localhost:9092'
}, {}, {topic: 'answer'});

function sendAnswer(data) {
    data = data.toString();
    let numbers = [];
    let result = false;
    for(let c of data) {
        if(c != " ") {
            c = Number(c);
        }
        if(Number.isInteger(c)) {
            numbers.push(c);
        }
    }
    if(numbers[0] + numbers[1] == numbers[2]) {
        result = true;
    }
    const answer = stream.write(Buffer.from(
        `Calculation ${numbers[0]} + ${numbers[1]} = ${numbers[2]} is ${result}`
    ));

    if(answer) {
        console.log('Message successfully to stream');
    } else {
        console.log('Problem writing to stream');
    }
}

const consumer = Kafka.KafkaConsumer({
    'group.id':'kafka',
    'metadata.broker.list':'localhost:9092'
}, {});

consumer.connect();

consumer.on('ready', () => {
    console.log('Consumer ready');
    consumer.subscribe(['task']);
    consumer.consume();
}).on('data', (data) => {
    //console.log(`Received message: ${data.value}`);
    sendAnswer(data.value);
});