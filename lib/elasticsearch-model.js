const _ = require('lodash');
const objtools = require('zs-objtools');
const pasync = require('pasync');
const SchemaModel = require('zs-unimodel').SchemaModel;
const XError = require('xerror');
const zstreams = require('zstreams');

const { elasticsearchQueryConvert } = require('./elasticsearch-query-convert');
const { createQuery } = require('./common-query');
const { schemaToMapping } = require('./schema-to-mapping');
const ElasticsearchDocument = require('./elasticsearch-document');


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

		// Array of extra indexes added to the schema as passed into schemaToMapping()
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
		this.mapping = schemaToMapping(this.schema, this.extraIndexes, this.options);
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
	 * @return {ElasticsearchDocument} - The new document.
	 */
	create(data = {}) {
		return new ElasticsearchDocument(this, data);
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
				query: esquery,
				facets: options.facets
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

	// Takes same additional options as find() below
	findStream(query, options = {}) {
		// Wait for this.connection.connectionWaiter.promise to resolve
		// Convert the given query to an ES query using elasticsearchQueryConvert()
		// Run a scroll that encapsulates each result using _createExisting() and wrap that
		// in a readable object stream.
		// Note that this function returns a stream, not a promise.  Because you first have to
		// wait for the connection promise to resolve, you'll need to use a trick to return a
		// stream before the scroll can be set up.  To do this, instantiate a passthrough object
		// stream and return that synchronously, and when the promise resolves, pipe the actual
		// scroll stream into the passthrough stream.  If the promise rejects, emit an error on the
		// passthrough stream.

		this.normalizeQuery(query);
		let esquery = elasticsearchQueryConvert(query);

		// We will return this passthrough, so we can write scrolling data to it later
		let pass = new zstreams.PassThrough({ objectMode: true });

		Promise.all([ // Ensure the ES Client and Index we're hittings are available
			this.getClient(),
			this._ensureIndex(options.index || this.defaultIndex)
		])
			.then(([ client, index ]) => {
				let isDone = false;
				let scrollId = null;
				return pasync.whilst(() => {
					return !isDone;
				}, () => {
					let scrollParams;
					if (scrollId) {
						// Continue the existing scroll
						scrollParams = {
							scrollId,
							scroll: options.scroll || '30s'
						};
					} else {
						// New scroll
						scrollParams = this._buildSearchParams(esquery, index, options);
						scrollParams.scroll = options.scroll || '30s';
						scrollParams.searchType = (scrollParams.sort) ? 'query_then_fetch' : 'scan';
					}
					return client.scroll(scrollParams).then((/*resp*/) => {
						///deal with it!
						console.log('let me out Im dying in a vat!~!!!');
					});
				})
				.catch((err) => {
					// Try to clear the scroll if it exists
					if (!scrollId) { return Promise.reject(err); }
					return client.clearScroll({ scrollId })
						.then(() => Promise.reject(err));
				});
			})
			.catch((err) => pass.emit('error', err));

		return pass;
	}

	/**
	 * Find documents.
	 *
	 * @method find
	 * @since v0.0.1
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
		query = this.normalizeQuery(query);
		let esquery = elasticsearchQueryConvert(query, this);
		return Promise.all([ // Ensure the ES Client and Index we're hittings are available
			this.getClient(),
			this._ensureIndex(options.index || this.defaultIndex)
		])
			.then(([ client, index ]) => {
				let searchParams = this._buildSearchParams(esquery, index, options);
				return client.search(searchParams);
			})
			.then((resp) => {
				let hits = objtools.getPath(resp, 'hits.hits', []);
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

	// In addition to base unimodel options, also take `index`, `routing`, and `parent` string parameters.
	insert(/*data, options = {}*/) {

	}

	// Takes same additional options as insert()
	insertMulti(/*datas, options = {}*/) {

	}

	// Takes same additional options as find()
	count(/*query, options = {}*/) {

	}

	// Note:  No need to implement aggregate() because the base class will call aggregateMulti()
	aggregateMulti(/*query, aggregates, options = {}*/) {
		// Wait for this.connection.connectionWaiter.promise to resolve
		// If the aggregate is a plain object (and not a common-query Aggregate object) convert it
		// Use the convertAggregate() function in elasticsearch-aggregate-convert to convert to an ES aggregate format
		// Execute the aggregate
		// Convert the result back to a common-query result aggregate format using convertAggregateResult()
	}

	// Takes same additional options as find()
	remove(/*query, options = {}*/) {

	}

	// Takes same additional options as find()
	update(query, update, options = {}) {
		update = this.normalizeUpdate(update);
		return this.findStream(query, options)
			.each((doc) => {
				// Apply update to each doc, then save
				update.apply(doc.getData(), { skipFields: options.skipFields });
				return doc.save();
			});
	}

	// This is the same as super.normalizeQuery, but we are using our own `createQuery`
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
