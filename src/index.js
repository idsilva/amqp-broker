import amqp from 'amqplib';
import { ReaderChannel, PublisherChannel } from './Channel'

class QueueServer {
    constructor(options = {}) {
        const { name,
            consumerQueue,
            publisherQueue,
            url,
            readerFunction,
            consumerExchange,
            publisherExchange,
            consumerPrefetch } = options
        this.queueUrl = url
        this.name = name || 'Server';
        this.consumerQueue = consumerQueue || '';
        this.consumerExchange = consumerExchange;
        this.publisherQueue = publisherQueue || '';
        this.readerFunction = readerFunction;
        this.consumerPrefetch = consumerPrefetch;
        this.publisherExchange = publisherExchange;
    }

    start = async () => {
        try {
            console.log(`[AMQP-${this.name}] Server: Establishing connection`);
            this.amqpConn = await this.connect();

            console.log(`[AMQP-${this.name}] Server: Channel Starter`);
            const [readerChannel, publisherChannel] = await Promise.all(
                [this.startReaderChannel(),
                this.startPublisherChannel()]);
            this.readerChannel = readerChannel;
            this.publisherChannel = publisherChannel;

        } catch (error) {
            console.log(error)
            this.amqpConn.close();
        }
    }


    connect = async () => {

        let connection = await amqp.connect(this.queueUrl + "?heartbeat=60", {});


        connection.on("error", (error) => {
            if (error.message !== "Connection closing") {
                console.error(`[AMQP-${this.name}] conn error:`, error.message);
                this.close()
            }
        });
        connection.on("close", this.onClose(this));

        console.log(`[AMQP-${this.name}] Server: Connection Established`);
        return connection
    }
    onClose = (server) => () => {
        console.error(`[AMQP-${server.name}] connection closed.`);
        return setTimeout(server.connect, 1000);
    }

    // A worker that acks messages only if processed successfully
    startReaderChannel = async () => {
        if (!this.consumerExchange && !this.consumerQueue) {
            return null
        }

        let channelInstance
        try {
            channelInstance = await this.amqpConn.createChannel()
            const readerChannel = new ReaderChannel({
                prefetch: this.consumerPrefetch,
                channelInstance,
                queueName: this.consumerQueue,
                readerFunction: this.readerFunction,
                exchange: this.consumerExchange
            });

            console.log(`[AMQP-${this.name}] Server: Queue Starter`);
            await readerChannel.start()

            return readerChannel

        } catch (error) {
            channelInstance.close()
            throw error
        }
    }
    // A worker that acks messages only if processed successfully
    startPublisherChannel = async () => {
        if (!this.publisherExchange && !this.publisherQueue) {
            return null
        }
        try {
            const channelInstance = await this.amqpConn.createChannel()
            const publisherChannel = new PublisherChannel({
                channelInstance,
                exchange: this.publisherExchange,
                queueName: this.publisherQueue
            });

            console.log(`[AMQP-${this.name}] Server: Queue Starter`);
            await publisherChannel.start()

            return publisherChannel

        } catch (error) {
            throw error
        }
    }

    close = () => {
        console.error(`[AMQP-${this.name}] Server: Connection Close`);
        this.amqpConn.close();
    }

    publish = (routeKey) => async (message, msgOpts = {}) => {
        if (this.publisherChannel) {
            return await this.publisherChannel.publish(routeKey)(message)(msgOpts)
        } else {
            return false;
        }

    }
}
export default QueueServer;
