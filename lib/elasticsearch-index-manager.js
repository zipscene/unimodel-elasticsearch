// Copyright 2016 Zipscene, LLC
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

const ElasticsearchIndex = require('./elasticsearch-index');
const elasticsearchUtils = require('./utils');
const XError = require('xerror');

/**
 * This class maintains a mapping from index names to initialized ElasticsearchIndex objects.
 * It ensures that each index is initialized once when it is used.
 *
 * @class ElasticsearchIndexManager
 * @constructor
 * @param {ElasticsearchConnection} connection - The connection object to ElasticSearch
 * @param {Object} indexConfigs - This is a map from ElasticSearch glob expressions to index
 *   configurations for indexes matching those expressions.  For example:
 *   ```js
 *   {
 *	   animals: {
 *       shards: 8
 *     },
 *     'events_*': {
 *       shards: 16
 *     }
 *   }
 *   ```
 *   If multiple globs match the same index names, the first listed glob takes precedence.
 * @param {Object} [indexOptions] - Options to pass to the ElasticsearchIndex constructor
 *   for initializing indexes.
 */
class ElasticsearchIndexManager {

	constructor(connection, indexConfigs, indexOptions={}) {
		this.connection = connection;
		this.indexConfigs = indexConfigs;
		this.indexOptions = indexOptions;

		// This is a map from string index names to instantiated ElasticsearchIndex objects
		this.indexMap = {};
	}

	/**
	 * Fetches (and initializes if necessary) an ElasticsearchIndex by name.
	 *
	 * @method getIndex
	 * @param {String} name - Name of the index
	 * @return {Promise} - Resolves with the ElasticsearchIndex object when initialized
	 */
	getIndex(name) {
		if (name in this.indexMap) {
			return this.indexMap[name].indexWaiter.promise;
		} else {

			// Find a matching config
			let matchingConfig;
			for (let glob in this.indexConfigs) {
				if (elasticsearchUtils.elasticsearchGlobFilter(glob, [ name ]).length) {
					matchingConfig = this.indexConfigs[glob];
					break;
				}
			}
			if (!matchingConfig) {
				return Promise.reject(new XError(XError.DB_ERROR, `No matching ES index config found for: ${name}`));
			}

			this.indexMap[name] = new ElasticsearchIndex(name, this.connection, matchingConfig, this.indexOptions);
			return this.indexMap[name].indexWaiter.promise;

		}
	}

}

module.exports = ElasticsearchIndexManager;
