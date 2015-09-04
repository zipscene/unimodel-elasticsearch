const _ = require('lodash');
const { SchemaDocument } = require('zs-unimodel');
const objtools = require('zs-objtools');

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
		this.fields = fields;
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
	 */
	save() {
		let normalizedData;
		return Promise.resolve()
			.then(() => this.model.trigger('pre-normalize'))
			.then(() => {
				// Normalize the data into serialized values
				normalizedData = this.model.schema.normalize(this.data, { serialize: true });
			})
			.then(() => this.model.trigger('post-normalize'))
			.then(() => this.model.trigger('pre-save'))
			.then(() => {
				// Try to ensure an internal ElasticSearch ID is set
				if (this.getInternalId()) { return Promise.resolve(); }
				return this.model.getMapping()
					.then((mapping) => {
						if (mapping._id && mapping._id.path) {
							console.dir(mapping._id);
							this.setInternalId(objtools.getPath(normalizedData, mapping._id.path, undefined));
						}
					});
			})
			.then(() => {
				// Ensure we have an index to save this document in
				if (this.getIndexId()) { return Promise.resolve(); }
				return this.model.getIndex()
					.then((index) => this.setIndexId(index.getName()));
			})
			.then(() => {
				// Retreive the connection's ElasticSearch client
				return this.model.connection.getClient().then((client) => {
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
								body: normalizedData
							});
						})
						.then((indexResponse) => {
							// Set fields based on the response
							this.setInternalId(indexResponse._id);
							this.setIndexId(indexResponse._index);
							if (indexResponse._parent) {
								this.setParentId(indexResponse._parent);
							}
							if (indexResponse._routing) {
								this.setRouting(indexResponse._routing);
							}
						});
				});
			})
			.then(() => {
				this._originalData = objtools.deepCopy(this.data);
				this._originalFields = objtools.deepCopy(this.fields);
			})
			.then(() => this.model.trigger('post-save'))
			.then(() => this);
	}

	remove() {
		// - Remember to use this._originalFields instead of this.fields to get the id, index, and routing to remove
		// Execute pre/post remove hooks
		// Execute the ES remove document
		return Promise.resolve()
			.then(() => {
				// Ensure we can actually remove this document
				if (this._originalData === null || this._originalFields === null) {
					let msg = 'Cannot remove document that did not originally exist.';
					throw new XError(XError.INTERNAL_ERROR, msg);
				}
				if (!this._originalFields.id || !this._originalFields.index) {
					let msg = 'Cannot remove document without internal ID or Index fields';
					throw new XError(XError.INTERNAL_ERROR, msg);
				}
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
			.then(() => this);
	}

}

module.exports = exports = ElasticsearchDocument;
