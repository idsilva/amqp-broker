class QueuePublisher {
    constructor(queueName = '') {
        this.queueName = queueName;
    }
    publishingOpts = { persistent: true };

    startExchange = (channel) => async (exchange) => {

        const { exchangeName, exchangeType = 'direct', ...rest } = exchange;
        await channel.assertExchange(exchangeName, exchangeType, rest);
        return this.publish(channel)(exchangeName)
    }
    startQueue = async (channel) => {
        await channel.assertQueue(this.queueName)
        return this.publish(channel)('')
    }

    publish = (channel) => (exchangeName) => (routeKey) => (content) => async (headers = {}) => {
        try {
            let opts = { ...this.publishingOpts, headers }
            const ok = await channel.publish(exchangeName, routeKey, Buffer.from(JSON.stringify(content)), opts);
            return ok;
        } catch (error) {
            console.error("[AMQP] publish", error);
            channel.close();
        }
    }
}


export default QueuePublisher;