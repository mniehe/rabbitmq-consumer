const uuidV4 = require('uuid/v4');
const debug = require('debug');
const logger = debug('consumer:stream');

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
class StreamConsumer {

  /**
   * 
   * @param {*} channel Open channel object to RabbitMQ.
   * @param {string} queueName Queue to consume from.
   * @param {string} [consumerName] Name of the consumer.
   */
  constructor(channel, queueName, consumerName) {
    this.queueName = queueName;
    this.channel = channel;
    this._consumerTag = (consumerName === undefined) ? uuidV4() : `${consumerName}:${uuidV4()}`;
  }

  /**
   * Start consuming messages.
   * 
   * @param {processMessageCallback} func A function that will recieve an array of messages
   * and return a Promise that contains an array of messages to ack.
   */
  start(func, unackFunc) {
    const options = {
      consumerTag: this._consumerTag,
      noAck: false
    };

    logger(`Starting consumer ${this._consumerTag}...`);

    this.channel.consume(this.queueName, this._consume.bind(this, func, unackFunc), options);
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

    logger('Consumer stopped!');
  }

  /**
   * Process a batch of messages.
   * 
   * @todo Do something with rejected messages. Maybe forward to different queue?
   * 
   * @private
   * @param {processMessageCallback} func Function to use when processing messages.
   * @param {processMessageCallback} unackFunc Function to use when processing error messages.
   * @param {Object} message Message that is received.
   */
  _consume(func, unackFunc, message) {
    const result = func(message);

    if (result instanceof Promise) {
      result.catch((err) => {
        // console.error(err);
        if (unackFunc !== undefined) {
          logger('Passing message to error function')
          unackFunc(message);
        }
      }).then(() => {
        this.channel.ack(message);
      });
    } else {
      throw new SyntaxError('Non promise function passed to consumer.');
    }
  }
}

module.exports = StreamConsumer;
