
/**
 * This class represents a connection to an Elasticsearch cluster.
 *
 * @class ElasticsearchConnection
 * @constructor
 * @param {String} url - Base URL to the ElasticSearch instance or load balancer
 * @param {Object} [options={}]
 *   @param {Number} [options.port=9200]
 *   @param {Boolean} [options.initialize=true] - If set to false, connection isn't immediately
 *     attempted.
 *   @param {Boolean} [options.retry=1000000] - Continually retry connection on failure X number
 *     of times.
 */
class ElasticsearchConnection extends EventEmitter {

	constructor(url, options={}) {
		// Save these on the class
		this.url = url;
		this.options = options;

		// Create a promise that's resolved once the initialization is complete.
		// The reason it's done this way is so this promise can exist immediately but
		// initialization can be deferred.
		// This promise should be resolved with `this` .
		this.connectionPromise = new Promise((resolve, reject) => {
			this._connectionPromiseResolve = resolve;
			this._connectionPromiseReject = reject;
		});
		// We also need to store this so we know to reset this promise if it has already
		// been resolved or rejected.
		this._connectionPromiseComplete = false;

		// Call initialize
		if (options.initialize !== false) {
			this.initialize();
		}
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
	 * @return {Promise} - Resolve with `this` .
	 */
	initialize() {
		// If the connection promise has already completed, we need to reset it
		if (this._connectionPromiseComplete) {
			this._connectionPromiseComplete = false;
			this.connectionPromise = new Promise((resolve, reject) => {
				this._connectionPromiseResolve = resolve;
				this._connectionPromiseReject = reject;
			});
		}
		// Try to connect and resolve the connection promise if successful
		this._tryElasticsearchConnect()
			then(() => {
				// Resolve the promise and emit a global convenience event
				this._connectionPromiseComplete = true;
				this._connectionPromiseResolve(this);
				this.emit('connected', this);
			}, (err) => {
				// Reject the promise (to notify any queries etc waiting on it) and emit an error
				// event signifying the initialization failed.
				this._connectionPromiseComplete = true;
				this._connectionPromiseReject(err);
				this.emit('error', err);
			});

		// Return the connection promise, for convenience.
		return this.connectionPromise;
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

}

module.exports = ElasticsearchConnection;
