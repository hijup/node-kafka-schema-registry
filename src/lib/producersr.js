const avro = require('avsc')
const kafka = require('node-rdkafka')
require('isomorphic-fetch')

module.exports = class Producer {
  constructor(rdkafkaProducerConfig, registryUrl, schemas) {
    this.producer = new kafka.Producer(rdkafkaProducerConfig)
    this.registryUrl = registryUrl
    this.schemas = schemas
		this.schemaIds = {}
    this.types = {}
    this.MAGIC_NUMBER = 0

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
        .catch(err => {
          reject(err)
        })
    }))

    Promise.all(schemasFetch)
      .then(schemas => {
        this.schemaIds = schemas.reduce((acc, schema) => Object.assign(acc, {[schema.subject]: schema.id}), {})
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
	
	init() {
    this.processSchemas(() => {
      this.producer.setPollInterval(1000)
      this.producer
        .on('event.log', console.log)
        .on('event.error', console.error)
        .on('ready', arg => {
          console.log('producer ready', JSON.stringify(arg))
        })
        .on('delivery-report', (err, report) => {
          if (err) 
            console.error('Error when producer receive delivery report: ', err)
          else
            console.log('Delivery report: ', report)
        })
        .connect(err => {
          if (err)
            throw new Error(`Producer error when connecting to kafka ${err}`)
        })
    })
	}
	
	produce(topic, data, callback) {
    try {
      this.producer
        .on('ready', () => {
          if (this.schemaIds[topic] == null) {
            console.error('schemaId not found')
            if (callback && typeof callback === 'function')
              callback({
                message: 'schemaId not found'
              })
          } else {
            const buffer = this.toMessageBuffer(topic, data, 10240)
            this.producer.produce(topic, null, buffer, null, null)
            console.log('Successfully produce to kafka')
          }
        })
    } catch (err) {
      console.error(err)
      if (callback && typeof callback === 'function')
        callback(err)
    }
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
