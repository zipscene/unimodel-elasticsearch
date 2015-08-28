const { EventEmitter } = require('events');
const pasync = require('pasync');

const ElasticsearchIndexManager = require('./elasticsearch-index-manager');

/**
 * This class represents a connection to an Elasticsearch cluster.
 *
 * @class ElasticsearchConnection
 * @constructor
 * @param {String} url - Base URL to the ElasticSearch instance or load balancer
 * @param {Object} indexConfigs - Configuration for indexes on this connection; see
 *   comment on ElasticsearchIndexManager for details.
 * @param {Object} [options={}]
 *   @param {Boolean} [options.initialize=true] - If set to false, connection isn't immediately attempted.
 *   @param {Boolean} [options.retry=1000000] - Continually retry connection on failure X number of times.
 *   @param {Object} [options.indexOptions] - Options passed to index initialization
 *   @param {Number} [options.maxSockets] - The maximum number of sockets that can be used at once.
 */
class ElasticsearchConnection extends EventEmitter {

	constructor(url=null, indexConfigs={}, { initialize = true, retry = 1000000, indexOptions, maxSockets }={}) {
		super();

		// Save these on the class
		this.url = url;

		// Set the options with the default assignments
		this.options = {};
		this.setRetry(retry);
		this.setIndexOptions(indexOptions);
		this.setMaxSockets(maxSockets);

		// This waiter is resolved when initialization is complete.
		this.connectionWaiter = pasync.waiter();

		// Store an index manager instance on this connection to manage index initializations
		// for this connection.
		this.indexManager = new ElasticsearchIndexManager(this, indexConfigs, this.options.indexOptions);

		// Call initialize
		if (initialize !== false && url) {
			this.initialize();
		}
	}

	/**
	 * Set the URL used to connect.  Useful if this class is instantiated before the URL is known.
	 *
	 * @method setUrl
	 * @param {String} url
	 */
	setUrl(url) {
		this.url = url;
	}

	/**
	 * Sets the index configs used by the index manager.  This should not be used after a connection
	 * has been established and models have been used.
	 *
	 * @method setIndexConfigs
	 * @param {Object} indexConfigs
	 */
	setIndexConfigs(indexConfigs) {
		this.indexManager.indexConfigs = indexConfigs;
	}

	/**
	 * Set the optiosn, while respecting the default assignments.
	 *
	 * method setOptions
	 * @param {Object} [options={}]
	 *   @param {Boolean} [options.initialize=true] - If set to false, connection isn't immediately attempted.
	 *   @param {Boolean} [options.retry=1000000] - Continually retry connection on failure X number of times.
	 *   @param {Object} [options.indexOptions] - Options passed to index initialization
	 *   @param {Number} [options.maxSockets] - The maximum number of sockets that can be used at once.
	 */
	setOptions({ initialize = true, retry = 1000000, indexOptions, maxSockets } = {}) {
		this.options = { initialize, retry, indexOptions, maxSockets };
	}

	setIndexOptions(indexOptions) {
		this.options.indexOptions = indexOptions;
	}

	setRetry(retry = 1000000) {
		this.options.retry = retry;
	}

	setMaxSockets(maxSockets) {
		this.options.maxSockets = maxSockets;
	}

	/**
	 * Executes a request to ElasticSearch to check if it's actually up and available.
	 *
	 * @method _checkElasticSearchUp
	 * @private
	 * @return {Promise} - Resolves with undefined or rejects if ES is down
	 */
	_checkElasticsearchUp() {
		// Hit the ElasticSearch status endpoint and make sure it returns
	}

	/**
	 * Tries to connect to ES according to the current retry settings.
	 *
	 * @method _tryElasticsearchConnect
	 * @private
	 * @return {Promise}
	 */
	_tryElasticsearchConnect() {
		if (this.options.retry !== false  && this.options.retry !== 0) {
			return this._checkElasticsearchUp();
		} else {
			return pasync.retry({
				times: this.options.retry || 1000000,
				interval: 10000
			}, () => this._checkElasticsearchUp() );
		}
	}

	/**
	 * Initializes the ES connection.  Resolves `this.connectionPromise` when complete.
	 * Emits `connected` or `error` events as well.  By default, this is automatically
	 * called from the constructor.
	 *
	 * @method initialize
	 * @param {Object} [options=this.options]
	 *   @param {Boolean} [options.retry=1000000] - Continually retry connection on failure X number of times.
	 *   @param {Object} [options.indexOptions] - Options passed to index initialization
	 *   @param {Number} [options.maxSockets] - The maximum number of sockets that can be used at once.
	 * @return {Waiter} - Resolve with `this` .
	 */
	initialize(options = this.options) {
		this.options = options;
		// Reset the waiter so new requests that come in in the meantime are queued up
		this.connectionWaiter.reset();

		// Try to connect and resolve the connection promise if successful
		this._tryElasticsearchConnect()
			.then(() => {
				// Resolve the promise and emit a global convenience event
				this.connectionWaiter.resolve(this);
				this.emit('connected', this);
			}, (err) => {
				// emit an error
				// event signifying the initialization failed.
				this.emit('error', err);
			});

		// Return the connection promise waiter, for convenience.
		return this.connectionWaiter;
	}

	/**
	 * Send a request to ElasticSearch.
	 *
	 * @param {String} path - The HTTP path to request.  This may optionally start with a `/` .
	 * @param {Object} [options]
	 *   @param {String} [options.method='GET']
	 *   @param {Object} [options.headers={}] - Object containing headers
	 *   @param {Object} [options.qs={}] - Object containing query string parameters
	 *   @param {Object} [options.body] - Object containing body parameters
	 * @return {Promise} - Resolves with the parsed response body.  Reject with an ElasticsearchError.
	 */
	request(path, options={}) {
		// Use the standard node request() library, the options to this function mirror those
		// Note that you will need to set a few additional options (such as content type) and
		// do stuff like encode options.body to JSON.
		// Also check the current zsapi elasticsearch connection code.  There are a few non-obvious
		// considerations here, such as using an http agent that can support many parallel connections.
		// By default, only a few parallel connections are supported.  Figure out which options used by
		// the existing code are still relevant.

		// This needs to obey this.options.maxSockets; otherwise, the ES code will tend to overload the
		// socket pool.  To do this, you need to instantiate your own http agent.  Code for this is
		// currently at zs-api/lib/elasticsearch/index.js:28 and some of it lives in our modified
		// elastical (which we will no longer be using).  The version of request used by elastical
		// is ancient, and it's possible that the options for passing the agent have changed, so please
		// verify that the maxSockets/agent options are obeyed.
	}

	/**
	 * Send a request to ElasticSearch.  Returns the result as a stream of raw data.
	 *
	 * @param {String} path - The HTTP path to request.  This may optionally start with a `/` .
	 * @param {Object} [options]
	 *   @param {String} [options.method='GET']
	 *   @param {Object} [options.headers={}] - Object containing headers
	 *   @param {Object} [options.qs={}] - Object containing query string parameters
	 *   @param {Object} [options.body] - Object containing body parameters
	 * @return {RequestStream} - A zstreams RequestStream object (eg. from `zstreams.request()`)
	 */
	requestStream(path, options) {
	}

	/**
	 * Closes the connection to ElasticSearch.
	 *
	 * @method close
	 * @return {Promise}
	 */
	close() {
		// Because there is (currently) no cleanup, this function does nothing.
		// It's here so it gets called in the appropriate places, so if we need to do
		// cleanup in the future, there's a place to put it.
		return Promise.resolve();
	}

}

module.exports = ElasticsearchConnection;
