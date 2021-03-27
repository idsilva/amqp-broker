class QueueReader {
    constructor(opts = {}) {
        const { queueName, readerFunction } = opts
        this.queue = queueName
        this.bound = false;
        this.callbackFunction = readerFunction
    }

    readQueue = async (channel) => {
        if (!this.bound) {
            const assertion = await channel.assertQueue(this.queue, { durable: true });
            console.log(assertion)
            this.consumer = await channel.consume(assertion.queue, this.processMsg(channel), { exclusive: true });
            console.log(this.consumer)
            this.bound = true;
        } else {
            throw new Error('Queue already bound')
        }

    }

    readExchange = (channel) => async (opts) => {
        if (!this.bound) {
            const { exchangeName = '', exchangeType = 'direct', routeKey, ...rest } = opts;
            const exchange = await channel.assertExchange(exchangeName, exchangeType, rest);
            console.log('exchange=>', exchange)
            const assertion = await channel.assertQueue(this.queue, { durable: true });
            console.log('assertion=>', assertion)
            const boundQueue = await channel.bindQueue(assertion.queue, exchangeName, routeKey || assertion.queue)
            console.log("boundQueue=>", boundQueue)

            this.consumer = await channel.consume(assertion.queue, this.processMsg(channel), { exclusive: true });
            console.log(this.consumer)
            this.bound = true;
        } else {
            throw new Error('Queue already bound')
        }
    }

    processMsg = (channel) => async (msg) => {

        try {
            await this.callbackFunction(msg)
            await channel.ack(msg);
        } catch (error) {
            console.log(error)
            await channel.reject(msg, true);
        }

    }
}


export default QueueReader;