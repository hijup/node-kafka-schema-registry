const { Consumer } = require('../')

const consumer = new Consumer({
  'metadata.broker.list': 'localhost:9092',
  'group.id': 'play_group'
}, 'http://localhost:8081', ['example'])

consumer.consume(message => {
  console.log(message)
})

process.on('SIGINT', () => {
  consumer.disconnect(() => {
    process.exit()
  })
})