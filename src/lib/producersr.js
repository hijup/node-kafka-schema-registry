const avro = require('avsc')
const kafka = require('node-rdkafka')
require('isomorphic-fetch')

module.exports = class Producer {
  constructor(rdkafkaProducerConfig, registryUrl, schemas) {
    this.producer = new kafka.Producer(rdkafkaProducerConfig)
    this.registryUrl = registryUrl
    this.schemas = schemas
    
    this.topicsMeta = {}
    this.MAGIC_NUMBER = 0

    this.isReady = false
    this.preProduced = []

    this.init()
  }

  processSchemas() {
    const schemasFetch = this.schemas.map(schema => this.registerSchema(schema.name, schema))

    console.log('Processing schemas...')
    return Promise.all(schemasFetch)
      .then(responses => {
        const successCount = responses.filter(response => response.success).length
        const failed = responses.filter(response => !response.success).map((response, index) => `index: ${index}, message: ${response.message}`)
        console.log(`${successCount} success, total: ${schemasFetch.length}`)

        if (failed.length > 0)
          console.error(`${failed.length} failed`, failed.join('\n'))
      })
  }

  registerSchema(subject, schema) {
    const uri = `${this.registryUrl}/subjects/${subject}-value/versions`
		const body = JSON.stringify({ schema: JSON.stringify(schema) })
    const options = {
      method: 'POST',
      headers: { 'Content-Type': 'application/vnd.schemaregistry.v1+json' },
      body
    }
    
    return fetch(uri, options)
      .then(response => response.json())
      .then(data => {
        if (data.error_code && data.error_code >= 400) {
          this.topicsMeta[subject] = {
            error: data
          }
          return { success: false, ...data }
        } else {
          this.topicsMeta[subject] = {
            schemaId: data.id,
            type: avro.parse(schema, { wrapUnions: true})
          }
          return { success: true, ...data }
        }
      })
  }

  processPreProduced() {
    let success = 0
    let total = this.preProduced.length
    while (this.preProduced.length > 0) {
      const message = this.preProduced.shift()
      this.produce(...message)
        .then(() => {
          success++
        })
        .catch(console.error)
    }

    console.log(`${success}/${total} pre-produced messages produced successfully!`)
  }
	
	init() {
    this.processSchemas()
      .then(() => {
        this.producer.setPollInterval(1000)
        this.producer
          .on('event.log', console.log)
          .on('event.error', console.error)
          .once('ready', arg => {
            this.isReady = true
            console.log('producer ready', JSON.stringify(arg))
            this.processPreProduced()
          })
          .on('delivery-report', (err, report) => {
            if (err) 
              console.error('Error when producer receive delivery report: ', err)
            else
              console.log('Delivery report: ', report)
          })
          .on('disconnected', () => {
            this.isReady = false
            console.log('goodbye')
          })
          .connect(err => {
            if (err)
              throw new Error(`Producer error when connecting to kafka ${JSON.stringify(err)}`)
          })
      })
	}
	
	produce(topic, data) {
    return new Promise((resolve, reject) => {
      if (this.isReady) {        
        if (this.topicsMeta[topic] == null)
          return reject({
            error: true,
            message: `Can't produce with topic: ${topic}`
          })

        if (this.topicsMeta[topic].error)
          return reject({
            error: true,
            message: `Unable to produce, the schema was not successfully registered. ${this.topicsMeta[topic].error.message}`
          })

        try {
          const schemaId = this.topicsMeta[topic].schemaId
          const avroSchema = this.topicsMeta[topic].type
          const buffer = this.toMessageBuffer(schemaId, avroSchema, data, 10240)
          this.producer.produce(topic, null, buffer, null, null)
          return resolve({ message: 'Message produced'})
        } catch (err) {
          return reject(err)
        }
    } else {
        this.preProduced.push([topic, data])
        return resolve({ success: false, message: `Producer not yet ready, message quequed`})
      }
    })
  }
  
  disconnect() {
    this.producer.disconnect()
  }

  toMessageBuffer(schemaId, avroSchema, data, length = 1024) {
    const buffer = new Buffer(length)
    buffer[0] = this.MAGIC_NUMBER
    buffer.writeInt32BE(schemaId, 1)

    const pos = avroSchema.encode(data, buffer, 5);
    if (pos < 0)
      return this.toMessageBuffer(schemaId, avroSchema, length)

    return buffer.slice(0, pos)
  }
}
