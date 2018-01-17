let kafkasr = require('../')
let exampleSchema = require('./schema')
let schemas = [exampleSchema]

let producer = new kafkasr.Producer({
  'metadata.broker.list': 'localhost:9092',
  'queue.buffering.max.messages': 100000,
  'queue.buffering.max.ms': 1000,
  'batch.num.messages': 100000,
  'queue.buffering.max.kbytes': 100000,
  'dr_cb': true
}, 'http://localhost:8081', schemas)

for (let i = 0; i < 100; i++) {
  const message = {
    id: i,
    is_good: true,
    created_at: Date.now()
  }
    producer.produce('example', message)
      .then(console.log)
      .catch(console.error)
}

process.on('SIGINT', () => {
  producer.disconnect(() => {
    process.exit()
  })
})
