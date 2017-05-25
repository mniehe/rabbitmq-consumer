const uuidV4 = require('uuid/v4');
const debug = require('debug');
const logger = debug('consumer:batch');

/**
 * The process message function that will be passed to BatchConsumer#start().
 * 
 * @callback processMessageCallback
 * @param {Object[]} messages Array of messages from RabbitMQ.
 * @returns {Promise<Object[]>} A promise that resolves to an array of messages to acknowledge.
 */

/**
 * Class that represents will process messages in batches. Consumption of messages
 * will happen in batches of `interval` milliseconds with a maximum of `maxBatch`
 * per batch. If less than maxBatch is in the queue, it'll process whatever is available
 * during next interval.
 * 
 * @class
 */
class BatchConsumer {

  /**
   * 
   * @param {*} channel Open channel object to RabbitMQ.
   * @param {string} queueName Queue to consume from.
   * @param {string} [consumerName] Name of the consumer.
   * @param {number} [interval=1000] Time to wait inbetween processing in milliseconds.
   * @param {number} [maxBatch=500] Maximum amount of messages to process in one batch.
   */
  constructor(channel, queueName, consumerName, interval = 1000, maxBatch = 500) {
    this.queueName = queueName;
    this.queue = [];
    this.channel = channel;
    this.maxBatch = maxBatch;
    this._interval = interval;
    this._consumerTag = (consumerName === undefined) ? uuidV4() : `${consumerName}:${uuidV4()}`;
    this._processInterval = null;
    this._processFunc = null;
    this._unackFunc = null;
  }

  /**
   * Start consuming messages.
   * 
   * @param {processMessageCallback} func A function that will recieve an array of messages
   * and return a Promise that contains an array of messages to ack.
   * @param {processMessageCallback} unackFunc A function that will recieve all messages that 
   * were failed by `func` before acking the message back to RabbitMQ.
   */
  start(func, unackFunc) {
    const options = {
      consumerTag: this._consumerTag,
      noAck: false
    };

    logger(`Starting consumer ${this._consumerTag}...`);

    this.channel.consume(this.queueName, this._receiveMessage.bind(this), options);
    this._processFunc = func;

    if (unackFunc !== undefined) {
      this._unackFunc = unackFunc;
    }

    this._processInterval = setInterval(this._consume.bind(this, this._processFunc, this._unackFunc), this._interval);
  }

  /**
   * Stop accepting messages.
   * 
   * @todo Make sure entire queue is processed and not just one batch.
   * 
   * @param {boolean} [processRemaining=true] Stop accepting messages and consume remaining messages or
   * just stop accepting messages.
   */
  stop(processRemaining=true) {
    this.channel.cancel(this._consumerTag);

    // Need to check if interval is currently running before clearing?
    clearInterval(this._processInterval);

    if (processRemaining && this.queue.length > 0) {
      this._consume(this._processFunc, this._unackFunc);
    }

    logger('Consumer stopped!');
  }

  /**
   * Receives messages from RabbitMQ and adds them to the queue to be processed.
   * 
   * @private
   * @param {Object} message Message as received from RabbitMQ.
   */
  _receiveMessage(message) {
    this.queue.push(message);
  }

  /**
   * Process a batch of messages.
   * 
   * @todo Do something with rejected messages. Maybe forward to different queue?
   * 
   * @private
   * @param {processMessageCallback} func Function to use when processing messages.
   * @param {processMessageCallback} unackFunc Function to use when processing failed messages.
   */
  _consume(func, unackFunc) {
    const itemCount = (this.queue.length >= this.maxBatch) ? this.maxBatch : this.queue.length;
    const processList = this.queue.splice(0, itemCount);

    // Exit if nothing to process
    if (itemCount === 0) return;

    logger(`Processing ${itemCount} item(s) with ${this.queue.length} still left in queue...`);

    const result = func(processList);

    if (result instanceof Promise) {
      result.then((ackList) => {
        if (!Array.isArray(ackList)) {
          throw new SyntaxError('Return value from consume function is not an array.');
        }

        const ackTags = ackList.map((message) => message.fields.deliveryTag);
        const unackList = processList.filter((message) => ackTags.indexOf(message.fields.deliveryTag) < 0);

        logger(`Processed ${ackList.length} message(s) with ${unackList.length} error(s)!`);

        if (unackFunc !== null) {
          unackFunc(unackList);
        }

        for (const message of processList) {
          this.channel.ack(message);
        }
      });
    } else {
      throw new SyntaxError('Non promise function passed to consumer.');
    }
  }
}

module.exports = BatchConsumer;
