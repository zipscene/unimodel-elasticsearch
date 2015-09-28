const _ = require('lodash');
const objtools = require('zs-objtools');
const pasync = require('pasync');
const SchemaModel = require('zs-unimodel').SchemaModel;
const XError = require('xerror');

const { createQuery } = require('./common-query');
const { convertSchema, convertQuery, convertAggregate, convertAggregateResult } = require('./convert');
const ElasticsearchDocument = require('./elasticsearch-document');
const ElasticsearchDocumentStream = require('./elasticsearch-document-stream');


/**
 * This class represents a schema'd model stored in ElasticSearch.
 *
 * @class ElasticsearchModel
 * @constructor
 * @param {String} typeName - The name of the "type" (ie, collection) in ElasticSearch
 * @param {Object|Schema} schema - The common-schema schema mapped to this model
 * @param {String|ElasticsearchIndex} index - The default index to save and search into
 * @param {ElasticsearchConnection} connection - The connection to ElasticSearch
 * @param {Object} [options] - Can contain any options passed to common-schema.  Additionally:
 *   @param {String} [options.searchIndexes] - An ElasticSearch glob expression matching the indexes
 *     to search over when doing a find.  Defaults to `index`.
 *   @param {String} [options.parentType] - The type name of the parent document type in ES
 *   @param {Boolean} [options.sendMapping=true] - If set to false, the mapping is not sent to ES
 *     when saving objects.
 *   @param {String[]} [options.keys] - Ordered array of key fields as returned by `getKeys()`
 *   @param {Boolean} [options.initialize] - If set to false, do not initialize the model.
 */
class ElasticsearchModel extends SchemaModel {

	constructor(typeName, schema, index, connection, options = {}) {
		// Initialize superclass
		super(schema, options);

		// Set class variables
		this.typeName = typeName;
		this.defaultIndex = index;
		this.connection = connection;
		this.options = options;
		this.keys = null;

		// This is a map of index names to promises as returned by ElasticsearchIndex#addMapping() .
		// It is used to ensure multiple put mappings for the same type are not executed against
		// the same index at the same time.
		this.indexMappingPromises = {};

		// Array of extra indexes added to the schema as passed into convertSchema()
		this.extraIndexes = [];

		if (this.options.initialize !== false) {
			// Wrap in setImmediate so `model.index` can be called right after creation
			setImmediate(() => this.initialize());
		}
	}

	/**
	 * Adds an extra index to this type.
	 *
	 * @method index
	 * @param {String} field - Field path
	 * @param {Object} spec - An object in the form:
	 *   `{ index: 'analyzed', analyzer: 'english', name: 'mySubField' }`
	 *   These are passed to ES for the given field.  The `name` property is used for multifields.
	 * @return {ElasticsearchModel} - `this`
	 */
	index(field, spec) {
		// If we've already started initializing indexes with mappings, it's too late
		if (!_.isEmpty(this.indexMappingPromises)) {
			throw new XError(XError.INTERNAL_ERROR, 'Cannot add indexes to model after initialization has started');
		}

		// Add the index to the list of specs
		spec.field = field;
		this.extraIndexes.push(spec);
	}

	/**
	 * Get keys associated with this model.
	 * Fetches the keys as follows:
	 * - Allow a 'keys' option to be passed into the constructor, setting the array of keys.
	 *   If this is passed in, return that.
	 * - If that is not passed in, traverse the schema to find schema elements tagged with the
	 *   { key: true } flag.  Return an array of field names, in traversal order, with this flag.
	 *   This allows shorthand specification of keys.  Note that using this means that key fields
	 *   in the schema must be declared in the order from most specific from least specific.
	 * - If neither of those is passed in, treat the first field indexed as { index: true } as the
	 *   only key field.
	 * - If no fields are indexed, throw an exception that no keys are declared.
	 * The key is then cached, so the whole sequence only needs to run once.
	 *
	 * @method getKeys
	 * @return {String[]} - Paths to key properties.
	 */
	getKeys() {
		if (this.keys) {
			return this.keys;
		}
		// Try to find keys
		let keys;
		if (this.options.keys) {
			keys = this.options.keys;
		} else {
			// Try to find { key: true } properties in the schema
			let subschemaKeys = [];
			this.schema.traverseSchema({
				onSubschema: (subschema, path) => {
					if (subschema.key) {
						subschemaKeys.push(path);
					}
				}
			});
			if (subschemaKeys.length) {
				// We found some subschema keys!
				keys = subschemaKeys;
			} else {
				// Try to find the first { index: truthy } subschema, and use that as a key
				let indexKey;
				this.schema.traverseSchema({
					onSubschema: (subschema, path) => {
						if (subschema.index && !indexKey) {
							indexKey = path;
						}
					}
				});
				if (indexKey) {
					// We found an index key!
					keys = [ indexKey ];
				}
			}
		}
		if (!keys) {
			// No keys were found
			throw new XError(XError.INTERNAL_ERROR, 'No keys are declared for this model');
		}
		this.keys = keys;
		return keys;
	}

	/**
	 * Initialize the ElasticSearch Index/Mapping.
	 *
	 * @method initialize
	 * @return {Promise{ElasticsearchIndex}} - Resolves with the default ElasticsearchIndex with
	 *   the mapping initialized.
	 */
	initialize() {
		// Right now all we have to do is ensure the mapping on the default index
		return Promise.resolve()
			.then(() => this._ensureIndex(this.defaultIndex))
			.then((index) => this._ensureMapping(index).then(() => index));
	}

	/**
	 * Convenience function to get the ElasticSearch Client instance for this Model
	 *
	 * @method getClient
	 * @return {Promise{elasticsearch.Client}}
	 */
	getClient() {
		return this.initialize()
			.then(() => this.connection.getClient());
	}

	/**
	 * Ensure the ElasticsearchIndex exists.
	 *
	 * @method _ensureIndex
	 * @private
	 * @param {String|ElasticsearchIndex} - The elasticsearch index to check.
	 * @return {Promise{ElasticsearchIndex}} - Resovles with the elasticsearch index.
	 */
	_ensureIndex(index) {
		if (!_.isString(index)) {
			// This is already an ElasticsearchIndex
			return Promise.resolve(index);
		}
		return this.connection.indexManager.getIndex(index);
	}

	/**
	 * Get the default index associated with this model.
	 *
	 * @method getIndex
	 * @return {Promise{ElasticsearchIndex}} - The default elasticsearch index.
	 */
	getIndex() {
		return this._ensureIndex(this.defaultIndex);
	}

	/**
	 * Get the ElasticSearch mapping once we have ensured it has at least been propogated to
	 * the default index.
	 *
	 * @method getMapping
	 * @return {Promise{Object}} - The ES Mapping
	 */
	getMapping() {
		return this._ensureIndex(this.defaultIndex)
			.then((index) => this._ensureMapping(index))
			.then(() => this.mapping);
	}

	/**
	 * Ensures that the given index has been initialized with this model's mapping.
	 *
	 * @method _ensureMapping
	 * @private
	 * @param {ElasticsearchIndex} index - The index to initialize with the mapping.
	 * @return {Promise} - Resolves with the ElasticsearchIndex object.
	 */
	_ensureMapping(index) {
		if (this.indexMappingPromises[index.name]) {
			// Mapping promise already exists
			return this.indexMappingPromises[index.name];
		}
		// Add the mapping
		this.mapping = convertSchema(this.schema, this.extraIndexes, this.options);
		let mappingPromise = index.addMapping(this.typeName, this.mapping);
		this.indexMappingPromises[index.name] = mappingPromise;
		return mappingPromise;
	}

	/**
	 * Get the type name of this model.
	 *
	 * @method getName
	 * @return {String} - The type name.
	 */
	getName() {
		return this.typeName;
	}

	/**
	 * Create a ElasticsearchDocument with the given data.
	 *
	 * @method create
	 * @param {Object} data - Common Schema-style data.
	 * @param {Object} fields - Internal ElasticSearch-specific fields from this Document
	 * @return {ElasticsearchDocument} - The new document.
	 */
	create(data = {}, fields = {}) {
		return new ElasticsearchDocument(this, data, fields);
	}

	/**
	 * Create an ElasticsearchDocument that represents an existing document in the database.  The data
	 * block given must include the meta-fields _id, _parent, _index, and (if applicable) _routing .
	 *
	 * @method _createExisting
	 * @private
	 * @param {Object} data - The document data (corresponding to _source)
	 * @param {Object} fields - Extra metadata fields that apply to the document, including
	 *   _routing, _parent, _index, _id, etc.
	 * @param {Boolean} [isPartialDocument=false] - If the document only contains a subset
	 *   of fields, this should be set to true.
	 * @return {ElasticsearchDocument}
	 */
	_createExisting(hit, isPartialDocument = false) {
		return ElasticsearchDocument.fromESData(this, hit, isPartialDocument);
	}

	/**
	 * Build "search" params for most elasticsearch.Client operations
	 *
	 * @method _buildSearchParams
	 * @private
	 * @param {Object} esquery
	 * @param {ElasticsearchIndex} index
	 * @param {Object} options - Common elasticsearch/unimodel find options.
	 * @return {Obejct} elasticsearch.Client search params.
	 */
	_buildSearchParams(esquery, index, options = {}) {
		let params = {
			type: this.getName(),
			index: index.getName(),
			from: options.skip || 0,
			fields: [ '_parent', '_routing', '_index', '_type', '_id', '_score' ],
			size: options.limit || 10,
			sort: options.sort,
			routing: options.routing,
			body: {
				query: esquery
			}
		};

		// Handle "fields"
		if (!_.isEmpty(options.fields)) {
			// Handle partial fields
			let include = [];
			let exclude = [];
			for (let field in options.fields) {
				if (options.fields[field]) {
					include.push(field);
				} else {
					exclude.push(field);
				}
			}
			if (include.length) {
				params._sourceInclude = include;
			}
			if (exclude.length) {
				params._sourceExclude = exclude;
			}
		} else {
			// Otherwise, include the full source
			params._source = true;
		}

		// Handle "sort"
		if (!_.isEmpty(options.sort)) {
			let sort = [];
			for (let field in options.sort) {
				let direction = (options.sort[field] === 1) ? 'asc' : 'desc';
				sort.push(`${field}:${direction}`);
			}
			params.sort = sort;
		}

		return params;
	}

	/**
	 * Build options object for "save" operations
	 *
	 * @method _buildSaveParams
	 * @private
	 * @param {Object} [options] - Options dealing with "save" operations
	 *   @param {String}
	 *   @param {String} [options.consistency] - Save consistency. Could be: "one", "quorum", "all"
	 *   @param {Boolean} [options.refresh] - If true, refresh the index after saving.
	 *   @param {String} [options.replication] - Replication types. Could be: "sync", "async"
	 */
	_buildSaveParams(options) {
		return {
			consistency: options.consistency,
			refresh: options.refresh,
			replication: options.replication
		};
	}


	/**
	 * Find documents in a stream.
	 *
	 * @method findStream
	 * @since v0.0.1
	 * @throws {QueryValidationError} - When an invalid Common Query style query is provided
	 * @param {Object|Query} query - Common-Query query to execute
	 * @param {Object} [options] - Additional options
	 *   @param {Number} options.skip - Number of documents to skip when returning results
	 *   @param {Number} options.limit - Maximum number of results to return
	 *   @param {String[]} options.fields - Array of dot-separated field names to return
	 *   @param {String[]} options.sort - An array of field names to sort by.  Each field can be
	 *     prefixed by a '-' to sort in reverse.
	 *   @param {String} [options.index] - A single index name to search, or an index glob expression.
	 *     Defaults to options.searchIndexes passed into the model constructor.
	 *   @param {String} [options.routing] - Optional routing parameter for search.
	 *   @param {String} [options.scrollTimeout='10m'] - Timeout before the scroll is deleted
	 *   @param {Number} [options.scrollSize=100] - Size of each pull from ElasticSearch
	 * @return {ReadableStream{Document}} - Resolves with a stream, that will have documents come down.
	 */
	findStream(query, options = {}) {
		// Normalize/validate/transform the query (find early issues)
		query = this.normalizeQuery(query);
		let esquery = convertQuery(query, this);

		// We will return this passthrough, so we can write scrolling data to it later
		let docStream = new ElasticsearchDocumentStream(this, !_.isEmpty(options.fields));
		// Set the limit that will be used to cut the stream off later
		const limit = _.isNumber(options.limit) ? options.limit : -1;

		Promise.all([ // Ensure the ES Client and Index we're hittings are available
			this.getClient(),
			this._ensureIndex(options.index || this.defaultIndex)
		])
			.then(([ client, index ]) => {
				let isDone = false;
				let progress = 0;
				let scrollId = null;
				let scroll = options.scrollTimeout || '10m'; // timeout for consistent view
				return pasync.whilst(() => {
					return !isDone;
				}, () => {
					let scrollPromise;
					if (scrollId) {
						// Continue the existing scroll
						scrollPromise = client.scroll({ scrollId, scroll });
					} else {
						// New scroll search
						let searchParams = this._buildSearchParams(esquery, index, options);
						searchParams.size = options.scrollSize || 100;
						searchParams.scroll = scroll; // timeout
						searchParams.searchType = (searchParams.sort) ? 'query_then_fetch' : 'scan';
						scrollPromise = client.search(searchParams);
					}
					return scrollPromise.then((resp) => {
						// Extract "hits" from the response
						let hits = objtools.getPath(resp, 'hits.hits') || [];

						// Do different actions based on if this is the initial "scroll" search
						if (!scrollId) {
							// Get the total as the total number of hits we would get without limit/skip
							docStream.setTotal(objtools.getPath(resp, 'hits.total') || 0);
						} else if (!hits.length) {
							// This is not the first scroll, but we have no results. We're done!
							isDone = true;
							return Promise.resolve();
						}

						// Get the scrollId (it shouldn't change once we get it the first time)
						scrollId = resp._scroll_id; //eslint-disable-line camelcase

						return pasync.each(hits, (hit) => new Promise((resolve, reject) => {
							docStream.write(hit, (err) => {
								if (err) { return reject(err); }
								progress++;
								resolve();
							});
						})).then(() => {
							// Check to see if we're done passing documents through with a limit
							isDone = limit > 0 && (progress >= limit);
						});
					});
				})
				.catch((err) => {
					// Try to clear the scroll if it exists
					if (!scrollId) { return Promise.reject(err); }
					return client.clearScroll({ scrollId })
						.then(() => Promise.reject(err));
				});
			})
			.catch((err) => docStream.emit('error', err))
			.then(() => docStream.end())
			.catch(pasync.abort);

		return docStream;
	}

	/**
	 * Find documents.
	 *
	 * @method find
	 * @since v0.0.1
	 * @throws {QueryValidationError} - When an invalid Common Query style query is provided
	 * @param {Object|Query} query - Common-Query query to execute
	 * @param {Object} [options] - Additional options
	 *   @param {Number} options.skip - Number of documents to skip when returning results
	 *   @param {Number} options.limit - Maximum number of results to return
	 *   @param {String[]} options.fields - Array of dot-separated field names to return
	 *   @param {Boolean} options.total - If true, also return a field with total number of results
	 *   @param {String[]} options.sort - An array of field names to sort by.  Each field can be
	 *     prefixed by a '-' to sort in reverse.
	 *   @param {String} [options.index] - A single index name to search, or an index glob expression.
	 *     Defaults to options.searchIndexes passed into the model constructor.
	 *   @param {String} [options.routing] - Optional routing parameter for search.
	 * @return {Promise{ElasticsearchDocument[]}} - Resolves with an array of result documents.
	 *   Rejects with an XError.
	 *   If the option `total` was set to true, the array also contains an additional member called
	 *   `total` containing the total number of results without skip or limit.
	 */
	find(query, options = {}) {
		// Normalize/validate/transform the query (find early issues)
		query = this.normalizeQuery(query);
		let esquery = convertQuery(query, this);

		return Promise.all([ // Ensure the ES Client and Index we're hittings are available
			this.getClient(),
			this._ensureIndex(options.index || this.defaultIndex)
		])
			.then(([ client, index ]) => {
				let searchParams = this._buildSearchParams(esquery, index, options);
				return client.search(searchParams);
			})
			.then((resp) => {
				let hits = objtools.getPath(resp, 'hits.hits') || [];
				let docs = [];
				for (let hit of hits) {
					docs.push(this._createExisting(hit, !_.isEmpty(options.fields)));
				}
				if (options.total) {
					docs.total = objtools.getPath(resp, 'hits.total');
				}
				return docs;
			});
	}

	/**
	 * Insert multiple documents into the database.
	 * NOTE: insert will call insertMulti for its implementation
	 *
	 * @method insertMulti
	 * @since v0.0.1
	 * @param {Array{Object}} datas - The data to insert as the document.
	 * @param {Object} [options]
	 *   @param {String} [options.index] - A single index name to search, or an index glob expression.
	 *     Defaults to options.searchIndexes passed into the model constructor.
	 *   @param {String} [options.routing] - Optional routing parameter for search.
	 *   @param {String} [options.consistency] - Save consistency. Could be: "one", "quorum", "all"
	 *   @param {Boolean} [options.refresh] - If true, refresh the index after saving.
	 *   @param {String} [options.replication] - Replication types. Could be: "sync", "async"
	 * @return {Promise} - Resolves with undefined or rejects with XError.
	 */
	insertMulti(datas, options = {}) {
		if (_.isEmpty(datas)) { return Promise.resolve(); }
		let body = [];
		for (let data of datas) {
			// Object saying we want to index, then the data for the index
			body.push({ index: {} }, data);
		}

		return Promise.all([ // Ensure the ES Client and Index we're hittings are available
			this.getClient(),
			this._ensureIndex(options.index || this.defaultIndex)
		])
			.then(([ client, index ]) => {
				let searchParams = this._buildSearchParams({}, index, options);
				let saveParams = this._buildSaveParams(options);
				let bulkParams = objtools.merge({}, searchParams, saveParams);
				bulkParams.body = body;
				return client.bulk(bulkParams);
			})
			.then(() => {}); // clear promise value
	}

	/**
	 * Returns a count of the number of documents matching a query.
	 *
	 * @method count
	 * @since v0.0.1
	 * @param {Object} query
	 * @param {Object} [options]
	 *   @param {String} [options.index] - A single index name to search, or an index glob expression.
	 *     Defaults to options.searchIndexes passed into the model constructor.
	 *   @param {String} [options.routing] - Optional routing parameter for search.
	 * @return {Promise} - Resolves with the numeric count.  Rejects with an XError.
	 */
	count(query, options = {}) {
		// Normalize/validate/transform the query (find early issues)
		query = this.normalizeQuery(query);
		let esquery = convertQuery(query, this);

		return Promise.all([ // Ensure the ES Client and Index we're hittings are available
			this.getClient(),
			this._ensureIndex(options.index || this.defaultIndex)
		])
			.then(([ client, index ]) => {
				let searchParams = this._buildSearchParams(esquery, index, options);
				let countParams = _.pick(searchParams, [ 'body', 'index', 'type', 'routing' ]);
				return client.count(countParams);
			})
			.then((resp) => {
				return resp.count || 0;
			});
	}

	/**
	 * Removes all documents matching the given query.
	 *
	 * @method remove
	 * @since v0.0.1
	 * @param {Object} query - Query to match documents to remove.
	 * @param {Object} [options]
	 *   @param {String} [options.index] - A single index name to search, or an index glob expression.
	 *     Defaults to options.searchIndexes passed into the model constructor.
	 *   @param {String} [options.routing] - Optional routing parameter for search.
	 *   @param {String} [options.consistency] - Save consistency. Could be: "one", "quorum", "all"
	 *   @param {String} [options.replication] - Replication types. Could be: "sync", "async"
	 * @return {Promise} - Promise that resolves with the number of documents removed, or rejects with XError
	 */
	remove(query, options = {}) {
		// Normalize/validate/transform the query (find early issues)
		query = this.normalizeQuery(query);
		let esquery = convertQuery(query, this);

		return Promise.all([ // Ensure the ES Client and Index we're hittings are available
			this.getClient(),
			this._ensureIndex(options.index || this.defaultIndex)
		])
			.then(([ client, index ]) => {
				let searchParams = this._buildSearchParams(esquery, index, options);
				let saveParams = this._buildSaveParams(options);
				let deleteParams = objtools.merge({}, searchParams, saveParams);
				return client.deleteByQuery(deleteParams);
			})
			.then(() => {}); // clear promise value
	}

	/**
	 * Updates all documents matching a given query.
	 *
	 * @method update
	 * @since v0.0.1
	 * @throws {XError} - On validation of [query|update] or while applying the update.
	 * @param {Object|Query} query - The query to match documents
	 * @param {Object|Update} update - The Mongo-style update expression used to update documents.  By
	 *   default, if this object contains no keys beginning with '$', the update expression is
	 *   implicitly wrapped in a '$set' .
	 * @param {Object} [options={}]
	 *   @param {Boolean} [options.allowFullReplace] - If this is set to true, update expressions
	 *     that do not contain any operators are allowed, and result in complete replacement of
	 *     any matching documents.
	 *   @param {String} [options.index] - A single index name to search, or an index glob expression.
	 *     Defaults to options.searchIndexes passed into the model constructor.
	 *   @param {String} [options.routing] - Optional routing parameter for search.
	 *   @param {Boolean} [options.skipFields] - Fields to not update.
	 *   @param {String} [options.consistency] - Save consistency. Could be: "one", "quorum", "all"
	 *   @param {Boolean} [options.refresh] - If true, refresh the index after saving.
	 *   @param {String} [options.replication] - Replication types. Could be: "sync", "async"
	 * @return {Promise} - Resolves with the number of documents updated, or rejects with XError
	 */
	update(query, update, options = {}) {
		// Split options object into set of options for each discrete step
		let normalizeOpts = _.pick(options, [ 'allowFullReplace' ]);
		let findOpts = _.pick(options, [ 'index', 'routing' ]);
		let updateOpts = _.pick(options, [ 'skipFields' ]);
		let saveOpts = this._buildSaveParams(options);

		// Normalize the update (find early issues)
		update = this.normalizeUpdate(update, normalizeOpts);

		return this.findStream(query, findOpts).each((doc) => {
			// Apply update to each doc, then save
			update.apply(doc.getData(), updateOpts);
			return doc.save(saveOpts);
		}).intoPromise();
	}

	/**
	 * Run multiple aggregates against a query.
	 * NOTE: aggregate will call aggregateMulti for its implementation
	 *
	 * @method aggregateMulti
	 * @since v0.0.1
	 * @param {Object} query - Filter to restrict aggregates to
	 * @param {Object} aggregates - A map from aggregate names to aggregate specs. See the
	 *   README for details.
	 * @param {Object} [options]
	 * @return {Promise} - Resolves with a map from aggregate names (as in the aggregates parameter)
	 *   to aggregate result objects.
	 */
	aggregateMulti(query, aggregates, options = {}) {
		query = this.normalizeQuery(query);
		let esquery = convertQuery(query, this);

		let aggregations = {};
		for (let i = 0, len = aggregates.length; i < len; i++) {
			aggregates[i] = this.normalizeAggregate(aggregates[i]);
			aggregations[`aggr_${i}`] = convertAggregate(aggregates[i]);
		}

		return Promise.all([ // Ensure the ES Client and Index we're hittings are available
			this.getClient(),
			this._ensureIndex(options.index || this.defaultIndex)
		])
			.then(([ client, index ]) => {
				let searchParams = this._buildSearchParams(esquery, index, options);
				let aggrParams = _.pick(searchParams, [ 'index', 'routing', 'body', 'total' ]);
				aggrParams.size = 0;
				aggrParams.body.aggregations = aggregations;
				console.log(JSON.stringify(aggrParams, null, 2));
				return client.search(aggrParams);
			})
			.then((resp) => {
				console.log(JSON.stringify(resp, null, 2));
				let result = [];
				for (let i = 0, len = aggregates.length; i < len; i++) {
					let aggregation = resp.aggregations[`aggr_${i}`];
					result[i] = convertAggregateResult(aggregation, aggregates[i]);
				}
				return result;
			});
	}

	/**
	 * Normalizes and validates the query that is passed in.
	 *
	 * @method normalizeQuery
	 * @since v0.0.1
	 * @param {Query|Object} query - Query to normalize
	 * @param {Object} [options] - Additional options to pass to the common-query normalizer
	 * @return {Query} - The query object after normalization
	 */
	normalizeQuery(query, options = {}) {
		let normalizeOptions = objtools.merge(
			{},
			this.modelOptions || {},
			options,
			{ schema: this.schema }
		);
		if (_.isPlainObject(query)) {
			query = createQuery(query, normalizeOptions);
		} else {
			query.normalize(normalizeOptions);
		}
		return query;
	}

}

module.exports = ElasticsearchModel;
