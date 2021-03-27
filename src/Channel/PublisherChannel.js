import { QueuePubliser } from '../Queue'
import BaseChannel from './BaseChannel'

class PublisherChannel extends BaseChannel {
    constructor(opts = {}) {
        const { channelInstance, exchange, queueName } = opts
        super(channelInstance)
        this.exchangeOpts = exchange;
        this.queue = new QueuePubliser(queueName)
    }

    start = async () => {
        try {
            if (this.exchangeOpts) {
                this.publishToExchange = await this.queue.startExchange(this.channel)({ ...this.exchangeOpts, durable: true })
            } else if (this.queue.queueName) {
                this.publishToQueue = await this.queue.startQueue(this.channel)
            } else {
                throw new Error('Exchange or Queue Name must be defined ')
            }
        } catch (error) {
            throw error
        }
    }

    publish = (routeKey) => (message) => async (msgOpts) => {
        try {
            if (this.publishToExchange) {
                return await this.publishToExchange(routeKey)(message)(msgOpts)
            } else if (this.publishToQueue) {
                return await this.publishToQueue(this.queue.queueName)(message)(msgOpts)
            }
        } catch (error) {
            throw error
        }
    }
}

export default PublisherChannel