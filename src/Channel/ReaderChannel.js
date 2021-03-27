import { QueueReader } from '../Queue'
import BaseChannel from './BaseChannel'

class ReaderChannel extends BaseChannel {
    constructor(opts = {}) {
        const { channelInstance, queueName = '', readerFunction, exchange, prefetch } = opts
        super(channelInstance, prefetch)
        this.queue = new QueueReader({ queueName, readerFunction })
        this.exchangeOpts = exchange;
    }
    start = async () => {
        if (this.exchangeOpts) {
            await this.queue.readExchange(this.channel)(this.exchangeOpts)
        } else {
            await this.queue.readQueue(this.channel)
        }


    }
}

export default ReaderChannel