const elasticsearch = require('elasticsearch');
const { EventEmitter } = require('events');
const objtools = require('zs-objtools');
const pasync = require('pasync');

const ElasticsearchHttpConnector = require('./elasticsearch-http-connector');
const ElasticsearchIndexManager = require('./elasticsearch-index-manager');
const ElasticsearchError = require('./elasticsearch-error');

/**
 * This class represents a connection to an Elasticsearch cluster.
 *
 * @class ElasticsearchConnection
 * @constructor
 * @param {Object|String} clientOptions - Either elasticsearch.Client options object, or base URL to the
 *   ElasticSearch instance/load balancer. For client options, see the following URL:
 *   https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/configuration.html
 * @param {Object} [indexConfigs={}] - Configuration for indexes on this connection; see
 *   comment on ElasticsearchIndexManager for details.
 * @param {Object} [options.indexOptions={}] - Options passed to index initialization; see comment on
 *   ElasticsearchIndex for details.
 * @param {Object} [options={}]
 *   @param {Boolean} [options.initialize=true] - If set to false, connection isn't immediately attempted.
 */
class ElasticsearchConnection extends EventEmitter {

	constructor(clientOptions={}, indexConfigs={}, indexOptions={}, options={}) {
		super();

		// This waiter is resolved when initialization is complete.
		this.connectionWaiter = pasync.waiter();

		// Store an index manager instance on this connection to manage index initializations
		// for this connection.
		this.indexManager = new ElasticsearchIndexManager(this, indexConfigs, indexOptions);

		// Convert the clientOptions to an object, if necessary
		if (typeof clientOptions === 'string') {
			clientOptions = { host: clientOptions };
		}
		this.setClientOptions(clientOptions, options.initialize);
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
	 * Set the elasticsearch client options.
	 * These options are passed directly on to elasticsearch.Client. Important options are shown below.
	 * For more options, see the following URL:
	 * https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/configuration.html
	 *
	 * @method setClientOptions
	 * @param {Object} clientOptions
	 *   @param {String|String[]|Object|Object[]} [clientOptions.host='http://localhost:9200'] - The
	 *     elasticsearch host to connect to.
	 *   @param {String} [clientOptions.apiVersion='1.7'] - Version of the API to target.
	 *   @param {Number} [clientOptions.maxRetries=1000000] - How many times the client should try to connect
	 *     to other nodes before returning a ConnectionFault error.
	 *   @param {Number} [clientOptions.maxSockets=10] - Maximum number of concurrent requests that can be
	 *     made to any node.
	 * @param {Boolean} [initialize=true] - Initialize the client after setting options.
	 */
	setClientOptions(clientOptions, initialize=true) {
		this._clientOptions = clientOptions;
		if (initialize !== false) {
			this.initialize();
		}
	}

	/**
	 * Set the options, which will be pass on to ElasticsearchIndex.
	 *
	 * @method setIndexOptions
	 * @param {Object} indexOptions
	 */
	setIndexOptions(indexOptions) {
		this.indexManager.indexOptions = indexOptions;
	}

	/**
	 * Check ElasicSearch cluster health to ensure it is still avilable.
	 *
	 * @method _tryElasticsearchConnect
	 * @private
	 * @return {Promise}
	 */
	_tryElasticsearchConnect() {
		return this.client.cluster.health({ level: 'cluster' })
			.then(() => {}); // Throw away response
	}

	/**
	 * Initializes the ES connection.  Resolves `this.connectionPromise` when complete.
	 * Emits `connected` or `error` events as well.  By default, this is automatically
	 * called from the constructor.
	 *
	 * @method initialize
	 * @return {Waiter} - Resolve with initialized ElasticsearchConnection.
	 */
	initialize() {
		if (this.client) {
			// Close the old client if it exists
			this.client.close();
		}

		// Construct new client options
		let clientOptions = objtools.merge({
			apiVersion: '1.7',
			maxRetries: 1000000,
			maxSockets: 10,
			connectionClass: ElasticsearchHttpConnector,
			defer: () => {
				// Construct a defer out of a normal promise
				let p = {};
				p.promise = new Promise((resolve, reject) => {
					p.resolve = resolve;
					p.reject = reject;
				}).catch((err) => {
					// Automatically convert ES Error to an XError
					let { status, message } = err;
					return Promise.reject(ElasticsearchError.fromESError(status, message));
				});
				return p;
			}
		}, this._clientOptions);
		// Initialize a new client
		this.client = new elasticsearch.Client(clientOptions);

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
				this.connectionWaiter.reject(err);
				this.emit('error', err);
			});

		// Return the connection promise waiter, for convenience.
		return this.connectionWaiter;
	}

	/**
	 * Get a Connector instance from the underlying Client ConnectionPool.
	 *
	 * @return {Promise} - Resolves with Client Connector instance
	 */
	_getClientConnection() {
		return this.getClient()
			.then((client) => {
				return new Promise((resolve, reject) => {
					client.transport.connectionPool.select((err, connection) => {
						if (err) { return reject(err); }
						resolve(connection);
					});
				});
			});
	}

	/**
	 * Get the elasticsearch.Client instance from this connection once it is initialized.
	 *
	 * @return {Promise{elasticsearch.Client}} Resolves with this connection's client instance.
	 */
	getClient() {
		return this.connectionWaiter.promise.then((connection) => connection.client);
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
	request(params) {
		return this._getClientConnection().then((connection) => {
			return new Promise((resolve, reject) => {
				connection.request(params, (err, stream) => {
					if (err) { return reject(err); }
					resolve(stream);
				});
			});
		});
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
	 * @return {Promise} - Resolves with a zstreams object built from an IncomingRequestStream
	 */
	requestStream(params) {
		return this._getClientConnection().then((connection) => {
			return new Promise((resolve, reject) => {
				connection.requestStream(params, (err, stream) => {
					if (err) { return reject(err); }
					resolve(stream);
				});
			});
		});
	}

	/**
	 * Closes the connection to ElasticSearch.
	 *
	 * @method close
	 * @return {Promise}
	 */
	close() {
		if (this.client) {
			// Close open client connections
			this.client.close();
		}
		return Promise.resolve();
	}

}

module.exports = ElasticsearchConnection;
