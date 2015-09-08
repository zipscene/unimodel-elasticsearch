const _ = require('lodash');
const SchemaModel = require('zs-unimodel').SchemaModel;
const XError = require('xerror');

const schemaToMapping = require('./schema-to-mapping').schemaToMapping;
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
	_createExisting(data, fields, isPartialDocument = false) {
		return new ElasticsearchDocument(this, data, fields, true, isPartialDocument);
	}

	// Takes same additional options as find() below
	findStream(/*query, options = {}*/) {
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
	find(/*query, options = {}*/) {
		// Wait for this.connection.connectionWaiter.promise to resolve
		// Convert the given query to an ES query using elasticsearchQueryConvert()
		// Convert any options given to the equivalent ES options (ie, sort, fields, etc.)
		// Run an elasticsearch query/filter
		// Encapsulate the results using _createExisting().  Remember to set the isPartialDocument
		// flag if only a partial document was returned.
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
	update(/*query, update, options = {}*/) {
		// Instead of doing a proper ES atomic update, just do a findSteadm() to fetch all
		// matching documents, and for each one, manually apply the update expression and
		// resave it.
	}

}

module.exports = ElasticsearchModel;
