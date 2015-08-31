const { EventEmitter } = require('events');

/**
 * This class is instantiated for each Elasticsearch Index (ie, database) in use.
 * It is responsible for initializing the index in ES.
 *
 * @class ElasticsearchIndex
 * @constructor
 * @param {String} name - The name of this index in ES
 * @param {ElasticsearchConnection} connection - The connection to ES object
 * @param {Object} [config={}] - Configuration settings for the index
 *   @param {Number} [config.shards=5] - Number of shards to split the index into
 *   @param {Number} [config.replicas=1] - Number of replicas for each shard
 *   @param {Object} [config.warmers] - Object (in raw ES format) specifying index warmers.  Passed
 *     directly to ES.
 *   @param {Object} [config.aliases] - Object (in raw ES format) specifying index aliases.  Passed
 *     directly to ES.
 * @param {Object} [options] - Additional options passed to the constructor
 *   @param {Boolean} [options.initialize=true] - If set to false, the index is not automatically
 *     created or synchronized on class construction.
 *   @param {Boolean} [options.retry=1000000] - Continually retry initialize on failure X number
 *     of times.
 */
class ElasticsearchIndex extends EventEmitter {

	constructor(name, connection, config, options = {}) {
		super();

		// Save these values on the class so they're accessible later, and so if we don't
		// initialize right away, they can be used later.
		this.name = name;
		this.connection = connection;
		this.config = config;
		this.options = options;

		// Construct a promise on this class that resolves when the index is fully initialized
		// and available.
		this.indexWaiter = pasync.waiter();

		// Call initialize
		if (options.initialize !== false) {
			this.initialize();
		}
	}

	/**
	 * Tries, once, to initialize the index.
	 *
	 * @method _initializeOnce
	 * @return {Promise}
	 */
	_initializeOnce() {

	}

	/**
	 * Tries to initialize the index, including retries.
	 *
	 * @method _tryInitialize
	 * @private
	 * @return {Promise}
	 */
	_tryInitialize() {
		if (this.options.retry !== false  && this.options.retry !== 0) {
			return this._initializeOnce();
		} else {
			return pasync.retry({
				times: this.options.retry || 1000000,
				interval: 10000
			}, () => this._initializeOnce() );
		}
	}

	/**
	 * Sets up the index on the server, including setting the index configuration.
	 * This is normally automatically called on class construction.  When initialization
	 * is complete, this resolves `this.indexPromise` .
	 *
	 * @method initialize
	 * @return {Waiter} - Returns `this.indexWaiter`
	 */
	initialize() {
		// If the index promise has already completed, we need to reset it
		this.indexWaiter.reset();

		// Need to wait for the connection to complete
		this.connection.connectionWaiter.promise
			.then((connection) => {
				// Try to initialize the index (with retries)
				return this._tryInitialize();
			})
			.then(() => {
				this.indexWaiter.resolve(this);
				this.emit('connected', this);
			}, (err) => {
				this.emit('error', err);
			});

		// Return the connection promise, for convenience.
		return this.indexWaiter;
	}

	/**
	 * Adds/updates a mapping in this index.
	 *
	 * @method addMapping
	 * @param {String} typeName - The name of the elasticsearch Type
	 * @param {Object} mapping - The elasticsearch-formatted mapping
	 * @return {Promise} - Resolves with undefined, rejects with an ElasticsearchError
	 */
	addMapping(typeName, mapping) {
		// This should execute the Put Mapping ES call to send the mapping to ES
		// It should intelligently retry (up to some threshold) on retryable errors, and
		// fail out on fatal errors such as a mapping conflict.
		return this.indexWaiter.promise.then(() => {
			// ...
		});
	}

}

module.exports = ElasticsearchIndex;
