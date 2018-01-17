const avro = require('avsc')
const kafka = require('node-rdkafka')
require('isomorphic-fetch')

module.exports = class Producer {
  constructor(rdkafkaProducerConfig, registryUrl, schemas) {
    this.producer = new kafka.Producer(rdkafkaProducerConfig)
    this.registryUrl = registryUrl
    this.schemas = schemas
    this.schemaIds = {}
    this.failedSchemaIds = {}
    this.types = {}
    this.MAGIC_NUMBER = 0

    this.isReady = false
    this.preProduced = []

    this.init()
  }

  processSchemas(callback) {
    const schemasFetch = this.schemas.map(schema => new Promise((resolve, reject) => {
      const subject = schema.name
      this.types[subject] = avro.parse(schema, { wrapUnions: true })
      this.registerSchema(subject, schema)
        .then(data => {
          console.log('schemafetch', data)
          resolve({
            subject,
            ...data
          })
        })
        .catch(error => {
          if (error.error_code && error.error_code === 409)
            resolve({ error, subject, schema })
          else
            reject(error)
        })
    }))

    Promise.all(schemasFetch)
      .then(schemas => {
        let registeredSchemas = []
        let failedSchemas = []
        schemas.forEach(schema => {
          if (schema.error)
            failedSchemas.push(schema)
          else
            registeredSchemas.push(schema)
        })

        this.schemaIds = registeredSchemas.reduce((acc, schema) => Object.assign(acc, {[schema.subject]: schema.id}), {})
        this.failedSchemaIds = failedSchemas.reduce((acc, item) => Object.assign(acc, {[item.subject]: item.error}), {})

        if (callback && typeof callback === 'function')
          callback()
      })
      .catch(err => {
        throw new Error(`Error when fetching subject from Schema Registry: ${JSON.stringify(err)}`)
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
			.then(response => {
        if (response.status >= 400)
          return response.json()
            .then(err => Promise.reject(err))
        else
          return response.json()
      })
  }

  processPreProduced() {
    while (this.preProduced.length > 0) {
      const message = this.preProduced.shift()
      this.produce(...message)
    }
  }
	
	init() {
    this.processSchemas(() => {
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
	
	produce(topic, data, callback) {
    if (this.isReady)
      try {
        if (this.schemaIds[topic] === null) {
          let message

          if (this.failedSchemaIds[topic] !== null)
            message = `Unable to produce, the schema not successfully registered. Message: ${this.failedSchemaIds[topic].message}`
          else
            message = `schemaId not found!`

          if (callback && typeof callback === 'function')
            callback({ message })
        } else {
          const buffer = this.toMessageBuffer(topic, data, 10240)
          this.producer.produce(topic, null, buffer, null, null)
          // console.log('Successfully produce to kafka')
        }
      } catch (err) {
        console.error(err)
        if (callback && typeof callback === 'function')
          callback(err)
      }
    else
      this.preProduced.push([topic, data, callback])
  }
  
  disconnect() {
    this.producer.disconnect()
  }

  toMessageBuffer(topic, data, length = 1024) {
    const buffer = new Buffer(length)
    buffer[0] = this.MAGIC_NUMBER
    buffer.writeInt32BE(this.schemaIds[topic], 1)

    const pos = this.types[topic].encode(data, buffer, 5);
    if (pos < 0) {
      return this.toMessageBuffer(topic, length)
    }
    return buffer.slice(0, pos)
  }
}
