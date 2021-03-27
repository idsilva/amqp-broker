class BaseChannel {
    constructor(channelInstance, prefetch) {
        this.channel = channelInstance;
        this.channel.on("error", function (err) {
            console.error("[AMQP-Channel] channel error", err.message);
        });
        this.channel.on("close", function () {
            console.log("[AMQP-Channel] channel closed");
        });

        this.channel.prefetch(prefetch);
        console.log("[AMQP-Channel] Channel connected");

    }
}

export default BaseChannel