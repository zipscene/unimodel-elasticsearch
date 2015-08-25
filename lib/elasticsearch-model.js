const SchemaModel = require('zs-unimodel').SchemaModel;
const schemaToMapping = require('./schema-to-mapping').schemaToMapping;

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

		// This is a map of index names to promises as returned by ElasticsearchIndex#addMapping() .
		// It is used to ensure multiple put mappings for the same type are not executed against
		// the same index at the same time.
		this.indexMappingPromises = [];

		// Array of extra indexes added to the schema as passed into schemaToMapping()
		this.extraIndexes = [];
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
		if (this.indexMappingPromises.length) {
			throw new XError(XError.INTERNAL_ERROR, 'Cannot add indexes to model after initialization has started');
		}

		// Add the index to the list of specs
		spec.field = field;
		this.extraIndexes.push(field);
	}

	getKeys() {
		// Fetch the keys as follows:
		// - Allow a 'keys' option to be passed into the constructor, setting the array of keys.
		//   If this is passed in, return that.
		// - If that is not passed in, traverse the schema to find schema elements tagged with the
		//   { key: true } flag.  Return an array of field names, in traversal order, with this flag.
		//   This allows shorthand specification of keys.  Note that using this means that key fields
		//   in the schema must be declared in the order from most specific from least specific.
		// - If neither of those is passed in, treat the first field indexed as { index: true } as the
		//   only key field.
		// - If no fields are indexed, throw an exception that no keys are declared.
		// - In any case, cache the computed array of keys on this object so the whole sequence above
		//   doesn't have to be checked each time.
	}

	/**
	 * Ensures that the given index has been initialized with this model's mapping.
	 *
	 * @method _ensureMapping
	 * @private
	 * @param {String|ElasticsearchIndex} index - The index to initialize with the mapping
	 * @return {Promise} - Resolves with the ElasticsearchIndex object
	 */
	_ensureMapping(index) {
		// Make sure we have both an index name and the ElasticsearchIndex object
		let indexPromise, indexName;
		if (_.isString(index)) {
			indexName = index;
			indexPromise = this.connection.indexManager.getIndex(indexName);
		} else {
			indexName = index.name;
			indexPromise = Promise.resolve(index);
		}

		// Make sure the index is initialized with the mapping
		return indexPromise.then((index) => {
			if (this.indexMappingPromises[indexName]) {
				return this.indexMappingPromises[indexName];
			} else {
				let mapping = schemaToMapping(this.schema, this.extraIndexes, this.options);
				this.indexMappingPromises[indexName] = index.addMapping(this.typeName, mapping);
				return this.indexMappingPromises[indexName];
			}
		});
	}

	getName() {
		return this.typeName;
	}

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
	_createExisting(data, fields, isPartialDocument = false}) {
		return new ElasticsearchDocument(this, data, fields, true, isPartialDocument);
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
	}

	/**
	 * Find documents.
	 *
	 * @method find
	 * @param {Object|Query} - Common-query query
	 * @param {Object} [options] - See the base unimodel class for main options.
	 *   @param {String} [options.index] - A single index name to search, or an index glob expression.
	 *     Defaults to options.searchIndexes passed into the model constructor.
	 *   @param {String} [options.routing] - Optional routing parameter for search.
	 * @return {ElasticsearchDocument[]} - Find results.  See base unimodel documentation.
	 */
	find(query, options = {}) {
		// Wait for this.connection.connectionWaiter.promise to resolve
		// Convert the given query to an ES query using elasticsearchQueryConvert()
		// Convert any options given to the equivalent ES options (ie, sort, fields, etc.)
		// Run an elasticsearch query/filter
		// Encapsulate the results using _createExisting().  Remember to set the isPartialDocument
		// flag if only a partial document was returned.
	}

	// In addition to base unimodel options, also take `index`, `routing`, and `parent` string parameters.
	insert(data, options = {}) {

	}

	// Takes same additional options as insert()
	insertMulti(datas, options = {}) {

	}

	// Takes same additional options as find()
	count(query, options = {}) {

	}

	// Note:  No need to implement aggregate() because the base class will call aggregateMulti()
	aggregateMulti(query, aggregates, options = {}) {
		// Wait for this.connection.connectionWaiter.promise to resolve
		// If the aggregate is a plain object (and not a common-query Aggregate object) convert it
		// Use the convertAggregate() function in elasticsearch-aggregate-convert to convert to an ES aggregate format
		// Execute the aggregate
		// Convert the result back to a common-query result aggregate format using convertAggregateResult()
	}

	// Takes same additional options as find()
	remove(query, options = {}) {

	}

	// Takes same additional options as find()
	update(query, update, options = {}) {
		// Instead of doing a proper ES atomic update, just do a findSteadm() to fetch all
		// matching documents, and for each one, manually apply the update expression and
		// resave it.
	}

}

module.exports = ElasticsearchModel;
