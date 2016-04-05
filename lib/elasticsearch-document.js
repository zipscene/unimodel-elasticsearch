const _ = require('lodash');
const XError = require('xerror');
const objtools = require('zs-objtools');
const Profiler = require('zs-simple-profiler');
const { SchemaDocument } = require('zs-unimodel');
const ElasticsearchError = require('./elasticsearch-error');

const profiler = new Profiler('ElasticsearchDocument');

/**
 * A single document that lives in ElasticSearch.
 *
 * @class ElasticsearchDocument
 * @constructor
 * @param {ElasticsearchModel} model - The model this document was constructed from.
 * @param {Object} data - The main data this document encapsulates.  This corresponds to the
 *   _source field in the bare document.
 * @param {Object} [fields] - Extra metadata fields that apply to the document.  These metadata
 *   fields include _parent, _id, _routing, and _index .
 * @param {Boolean} [isExistingDocument=false] - If true, the data is treated as a partial slice
 *   of the document data and is not allowed to be saved.
 * @param {Boolean} [isPartialDocument=false] -
 */
class ElasticsearchDocument extends SchemaDocument {

	constructor(model, data, fields = {}, isExistingDocument = false, isPartialDocument = false) {
		super(model, data);

		// Save parameters on the class
		this.fields = objtools.deepCopy(fields);
		this._isPartialDocument = isPartialDocument;

		// Save "original" data in the document is already in the database so we can do diffs
		if (isExistingDocument) {
			this._originalData = objtools.deepCopy(data);
			this._originalFields = objtools.deepCopy(fields);
		} else {
			this._originalData = null;
			this._originalFields = null;
		}

		// Call the post-init hook, initialization has finished
		model.triggerSync('post-init', this);
	}

	/**
	 * Create an ElasticsearchDocument from raw Elasticsearch Document data.
	 *
	 * @method fromESData
	 * @static
	 * @param {ElasticsearchModel} model - The model to build this document for.
	 * @param {Object} esdata - Raw ElasticSearch hit data.
	 * @param {Boolean} [isPartialDocument=false] - If the document only contains a subset
	 *   of fields, this should be set to true.
	 * @return {ElasticsearchDocument} - The ElasticsearchDocument representing the raw data.
	 */
	static fromESData(model, esdata, isPartialDocument = false) {
		esdata.fields = esdata.fields || {};
		let fields = {
			id: esdata._id,
			index: esdata._index,
			routing: esdata.fields._routing,
			parent: esdata.fields._parent
		};
		return new ElasticsearchDocument(model, esdata._source, fields, true, isPartialDocument);
	}

	/**
	 * Returns the internal id field.
	 *
	 * @method getInternalId
	 * @return {String}
	 */
	getInternalId() {
		return this.fields.id;
	}

	/**
	 * Sets the internal id field.
	 *
	 * @method setInternalId
	 * @param {String} id
	 */
	setInternalId(id) {
		this.fields.id = id;
	}

	/**
	 * Returns the internal parent field.
	 *
	 * @method getParentId
	 * @return {String}
	 */
	getParentId() {
		return this.fields.parent;
	}

	/**
	 * Sets the internal parent field.
	 *
	 * @method setParentId
	 * @param {String} parent
	 */
	setParentId(parent) {
		this.fields.parent = parent;
	}

	/**
	 * Returns the internal routing field.
	 *
	 * @method getRouting
	 * @return {String}
	 */
	getRouting() {
		return this.fields.routing;
	}

	/**
	 * Sets the internal routing field.
	 *
	 * @method setRouting
	 * @param {String} routing
	 */
	setRouting(routing) {
		this.fields.routing = routing;
	}

	/**
	 * Returns the internal index field.
	 *
	 * @method getIndexId
	 * @return {String}
	 */
	getIndexId() {
		return this.fields.index;
	}

	/**
	 * Sets the internal index field.
	 *
	 * @method setIndexId
	 * @param {String} index
	 */
	setIndexId(index) {
		this.fields.index = index;
	}

	/**
	 * Get teh type of this document.
	 *
	 * @method getType
	 * @param {String} type
	 */
	getType() {
		return this.model.getName();
	}

	/**
	 * Save the document to the database
	 *
	 * @method save
	 * @param {Object} opts
	 *   @param {String} [opts.consistency] - Save consistency. Could be: "one", "quorum", "all"
	 *   @param {Boolean} [opts.refresh] - If true, refresh the index after saving.
	 *   @param {String} [opts.replication] - Replication types. Could be: "sync", "async"
	 * @return {Promise{ElasticsearchDocument}} - Resolves when the document has been saved.
	 */
	save(opts = {}) {
		let prof = profiler.begin('#save');

		// Validate opts if they exist
		if (!_.isEmpty(opts)) {
			if (opts.consistency) {
				if (!_.contains([ 'one', 'quorum', 'all' ], opts.consistency)) {
					let msg = 'Save consistency options must be one of: "one", "quorum", "all"';
					throw new XError(XError.INVALID_ARGUMENT, msg);
				}
			}
			if (opts.replication) {
				if (!_.contains([ 'sync', 'async' ], opts.replication)) {
					let msg = 'Replication types must be one of: "sync", "async"';
					throw new XError(XError.INVALID_ARGUMENT, msg);
				}
			}
		}

		let normalizedData;
		return Promise.resolve()
			.then(() => this.model.trigger('pre-normalize'))
			.then(() => {
				// Normalize the data into serialized values
				normalizedData = this.model.schema.normalize(this.data, { serialize: true });
			})
			.then(() => this.model.getMapping())
			.then((mapping) => {
				// Set internal fields (if they are not already set)
				if (!this.getInternalId() && mapping._id && mapping._id.path) {
					this.setInternalId(objtools.getPath(normalizedData, mapping._id.path));
				}
				if (!this.getRouting()) {
					if (mapping._parent && this.getParentId()) {
						// This is the default for documents with `_parent` set
						this.setRouting(this.getParentId());
					} else {
						// This is the default for all documents (without `_routing: { required: true }` set)
						// We're forcing this setting to get around null Parent IDs in "child" documents
						this.setRouting(this.getInternalId());
					}
				}
			})
			.then(() => this.model.trigger('post-normalize'))
			.then(() => this.model.trigger('pre-save'))
			.then(() => {
				// Ensure we have an index to save this document in
				if (this.getIndexId()) { return Promise.resolve(); }
				return this.model.getIndex()
					.then((index) => this.setIndexId(index.getName()));
			})
			.then(() => {
				// Retreive the connection's ElasticSearch client
				return this.model.getClient().then((client) => {
					return Promise.resolve()
						.then(() => {
							// Check if existing document needs to be purged first
							let internalFields = [ 'id', 'routing', 'index', 'parent' ];
							let needsPurged = !!this._originalFields && _.any(internalFields, (field) => {
								return (
									this._originalFields[field] &&
									this.fields[field] !== this._originalFields[field]
								);
							});
							if (!needsPurged) { return Promise.resolve(); }
							// Need to purge the unclean
							return client.delete({
								type: this.getType(),
								id: this._originalFields.id,
								index: this._originalFields.index,
								routing: this._originalFields.routing,
								parent: this._originalFields.parent
							}).catch((err) => {
								// Care for "does not exist"
								return Promise.reject(err);
							});
						})
						.then(() => {
							// Perform the index operation
							return client.index({
								type: this.getType(),
								id: this.fields.id,
								index: this.fields.index,
								parent: this.fields.parent,
								routing: this.fields.routing,
								body: normalizedData,
								fields: [ '_routing', '_parent', '_id', '_index' ],
								consistency: opts.consistency,
								refresh: opts.refresh,
								replication: opts.replication || 'sync'
							});
						})
						.then((esdata) => {
							// Set fields based on the response
							this.setInternalId(esdata._id);
							this.setIndexId(esdata._index);
							if (esdata.fields) {
								if (esdata.fields._parent) {
									this.setParentId(esdata.fields._parent);
								}
								if (esdata.fields._routing) {
									this.setRouting(esdata.fields._routing);
								}
							}
						});
				});
			})
			.then(() => {
				this._originalData = objtools.deepCopy(this.data);
				this._originalFields = objtools.deepCopy(this.fields);
			})
			.then(() => this.model.trigger('post-save'))
			.then(() => this)
			.then(prof.wrappedEnd());
	}

	/**
	 * Remove document from the database.
	 *
	 * @method remove
	 * @return {Promise{ElasticsearchDocument}} - Resovles when the document has been removed.
	 */
	remove() {
		let prof = profiler.begin('#remove');

		return Promise.resolve()
			.then(() => {
				// Ensure we can actually remove this document
				if (this._originalData === null || this._originalFields === null) {
					let msg = 'Cannot remove document that did not originally exist.';
					throw new ElasticsearchError(ElasticsearchError.DB_ERROR, msg);
				}
				if (!this._originalFields.id || !this._originalFields.index) {
					let msg = 'Cannot remove document without internal ID or index fields';
					throw new ElasticsearchError(ElasticsearchError.DB_ERROR, msg);
				}
				return this.model.getMapping().then((mapping) => {
					if (!this._originalFields.routing) {
						if (mapping._parent && this._originalFields.parent) {
							// Default for "child" documents
							this._originalFields.routing = this._originalFields.parent;
						} else {
							// Default for all other documents
							this._originalFields.routing = this._originalFields.id;
						}
					}
				});
			})
			.then(() => this.model.trigger('pre-remove'))
			.then(() => {
				// Get the connection's client
				return this.model.connection.getClient()
					// Run the actual delete
					.then((client) => client.delete({
						type: this.model.getName(),
						id: this._originalFields.id,
						index: this._originalFields.index,
						routing: this._originalFields.routing,
						parent: this._originalFields.parent
					}));
			})
			.then(() => this.model.trigger('post-remove'))
			.then(() => {
				// Update the data fields
				this._originalData = null;
				this._originalFields = null;
			})
			.then(() => this)
			.then(prof.wrappedEnd());
	}

}

module.exports = exports = ElasticsearchDocument;
