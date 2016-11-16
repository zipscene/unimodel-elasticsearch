// Copyright 2016 Zipscene, LLC
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

const _ = require('lodash');
const { EventEmitter } = require('events');
const pasync = require('pasync');

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
 * @param {Object} [options] - Additional options passed to the constructor
 *   @param {Boolean} [options.initialize=true] - If set to false, the index is not automatically
 *     created or synchronized on class construction.
 */
class ElasticsearchIndex extends EventEmitter {

	constructor(name, connection, config, options = {}) {
		super();

		// Save these values on the class so they're accessible later, and so if we don't
		// initialize right away, they can be used later.
		this.name = name;
		this.connection = connection;
		this.config = {
			shards: config.shards || 5, //eslint-disable-line camelcase
			replicas: config.replicas || 1, //eslint-disable-line camelcase
			warmers: config.warmers
		};
		this.options = options;

		// Construct a promise on this class that resolves when the index is fully initialized
		// and available.
		this.indexWaiter = pasync.waiter();

		// Call initialize
		this._initializing = false;
		this._initialized = false;
		if (options.initialize !== false) {
			this.initialize();
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
		// Ensure initialize is only run once (settings shouldn't change after initialization)
		if (this._initialized || this._initializing) {
			return this.indexWaiter;
		}
		this._initializing = true;

		// If the index promise has already completed (in which it failed to initialize), we need to reset it
		this.indexWaiter.reset();

		// Need to wait for the connection to complete
		this.connection.getClient().then((client) => {
			// Do initialization with the connection
			return client.indices.exists({ index: this.name }).then((exists) => {
				if (!exists) {
					// Create the index and set/create settings/warmers
					return this._initializeNewIndex(client);
				} else {
					// Update settings
					return this._initializeExistingIndex(client);
				}
			});
		}).then(() => {
			this._initializaing = false;
			this._initialized = true;
			this.indexWaiter.resolve(this);
			this.emit('connected', this);
		}, (err) => {
			this._initializaing = false;
			this.indexWaiter.reject(err);
			this.emit('error', err);
		});

		// Return the connection promise, for convenience.
		return this.indexWaiter;
	}

	/**
	 * Initialize a new client using the Index Create API
	 *
	 * @method _initializeNewIndex
	 * @private
	 * @param {elasticsearch.Client} client
	 * @return {Promise}
	 */
	_initializeNewIndex(client) {
		return client.indices.create({
			index: this.name,
			body: {
				settings: {
					index: {
						number_of_shards: this.config.shards, //eslint-disable-line camelcase
						number_of_replicas: this.config.replicas //eslint-disable-line camelcase
					}
				},
				warmers: this.config.warmers
			}
		});
	}

	/**
	 * Initialize an existing client by putting settings/warmers on the index.
	 *
	 * @method _initializeExistingIndex
	 * @private
	 * @param {elasticsearch.Client} client
	 * @return {Promise}
	 */
	_initializeExistingIndex(client) {
		return client.indices.putSettings({
			index: this.name,
			body: {
				index: {
					number_of_replicas: this.config.replicas //eslint-disable-line camelcase
				}
			}
		}).then(() => {
			// Create/update warmers
			let warmers = _.pairs(this.config.warmers);
			return pasync.eachSeries(warmers, ([ name, warmer ]) => {
				return client.indices.putWarmer({
					index: this.name,
					name,
					type: warmer.type || warmer.types,
					body: warmer.source
				});
			});
		});
	}

	getName() {
		return this.name;
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
		return this.indexWaiter.promise
			.then((/*index*/) => this.connection.connectionWaiter.promise)
			.then((connection) => {
				return connection.client.indices.putMapping({
					index: this.name,
					type: typeName,
					body: {
						[ `${typeName}` ]: mapping
					}
				});
			});
	}

}

module.exports = ElasticsearchIndex;
