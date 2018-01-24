const avro = require('avsc')
const kafka = require('node-rdkafka')
require('isomorphic-fetch')

module.exports = class Consumer {
  constructor(rdkafkaConsumerConfig, registryUrl, topicNames) {
    this.consumer = new kafka.KafkaConsumer(rdkafkaConsumerConfig)
    this.registryUrl = registryUrl
    this.topicNames = this.topicNameParser(topicNames)

    this.schemas = {}
  }

  topicNameParser(topicNames) {
    if (Array.isArray(topicNames))
      return topicNames
    else if (typeof topicNames === 'string')
      return [topicNames]
    else
      throw new Error(`Invalid topicName type. Expected string|array, got ${typeof topicNames}`)
  }

  consume(callback) {
    this.consumer
      .on('event.log', console.log)
      .on('event.error', console.error)
      .on('ready', arg => {
        console.log('consumer ready', JSON.stringify(arg))
        this.consumer
          .subscribe(this.topicNames)
          .consume()

          console.log(`topic names: ${this.topicNames.join(', ')}`)
      })
      .on('disconnected', () => {
        console.log('consumer stopped')
      })
      .on('data', message => {
        this.messageHandler(message, callback)
      })
      .connect()
  }

  messageHandler(message, callback) {
    const messageBuffer = new Buffer(message.value)
    const schemaId = messageBuffer.readInt32BE(1)

    this.getSchema(schemaId)
      .then(schema => {
        const parsedMessage = this.parseMessage(schema, messageBuffer)

        if (callback && typeof callback === 'function')
          callback(parsedMessage)
        else
          throw new Error('invalid callback function')
      })
  }

  getSchema(schemaId) {
    if (this.schemas[schemaId] != null) {
      console.log(`getting schema from memory. schemaId: ${schemaId}`)
      return Promise.resolve(this.schemas[schemaId])
    } else {
      console.log(`getting schema from ${this.registryUrl}. schemaId: ${schemaId}`)
      return fetch(`${this.registryUrl}/schemas/ids/${schemaId}`)
        .then(data => data.json())
        .then(data => {
          const schema = JSON.parse(data.schema)
          this.schemas[schemaId] = schema
          return schema
        })
    }
  }

  parseMessage(schema, messageBuffer) {
    const type = avro.parse(schema)
    const dataBuffer = messageBuffer.slice(5, messageBuffer.length)
    return type.fromBuffer(dataBuffer)
  }

  disconnect() {
    this.consumer.disconnect()
  }
}