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
	 * Returns the internal _id field.
	 *
	 * @method getInternalId
	 * @return {String}
	 */
	getInternalId() {
		return this.fields.id;
	}

	/**
	 * Sets the internal _id field.
	 *
	 * @method setInternalId
	 * @param {String} id
	 */
	setInternalId(id) {
		this.fields.id = id;
	}

	/**
	 * Returns the internal _parent field.
	 *
	 * @method getParentId
	 * @return {String}
	 */
	getParentId() {
		return this.fields.parent;
	}

	/**
	 * Sets the internal _parent field.
	 *
	 * @method setParentId
	 * @param {String} parent
	 */
	setParentId(parent) {
		this.fields.parent = parent;
	}

	/**
	 * Returns the internal _routing field.
	 *
	 * @method getRouting
	 * @return {String}
	 */
	getRouting() {
		return this.fields.routing;
	}

	/**
	 * Sets the internal _routing field.
	 *
	 * @method setRouting
	 * @param {String} routing
	 */
	setRouting(routing) {
		this.fields.routing = routing;
	}

	/**
	 * Returns the internal _index field.
	 *
	 * @method getIndexId
	 * @return {String}
	 */
	getIndexId() {
		return this.fields.index;
	}

	/**
	 * Sets the internal _index field.
	 *
	 * @method setIndexId
	 * @param {String} index
	 */
	setIndexId(index) {
		this.fields.index = index;
	}

	save() {
		// - Normalize data according to the schema
		// - Call pre-normalize, post-normalize, and pre-save hooks
		// - If this was an existing document (this._originalFields exists) and any of _id, _routing,
		//   _index, _parent fields have changed, then the old document must be removed first.
		// - Execute an `index` call on ElasticSearch to save the new document data.
		// - Set this._originalData and this._originalFields to deep-copied versions of this.data and this.fields
		// - Set the values of this.fields and this._originalFields to the values updated from the
		//   server (ie, the server will generate an _id) or at least from known values (ie, we
		//   already know the _index it was saved to)
		// - Execute the post-save hook

		let client, index, esData;
		return Promise.all([
			this.model.connection.getClient(),
			this.model.getIndex()
		])
			.then(([ _client, _index ]) => {
				client = _client;
				index = _index;
			})
			.then(() => this.model.trigger('pre-normalize'))
			.then(() => {
				let normalizedData = this.model.schema.normalize(this.data);
				esData = this.model.schema.transform(normalizedData, {
					onField: (field, value, schema, subschemaType) => {
						let type = subschemaType.getName();
						if (type === 'date') {
							if (value && _.isFunction(value.toISOString)) {
								return value.toISOString();
							}
						} else if (type === 'geopoint') {
							// TODO: validate geopoint

						}
						return value;
					}
					//TODO:where am IP?!??!

				});
				// TODO: transform into ES data
				console.dir(normalizedData);
				console.log(typeof normalizedData.found);
				//TODO: pull ID/Parent fields out of the data
			})
			.then(() => this.model.trigger('post-normalize'))
			.then(() => this.model.trigger('pre-save'))
			.then(() => {
				// Check if existing document needs to be purged first
				let needsPurged = !!this._originalFields && _.any([ 'id', 'routing', 'index', 'parent' ], (field) => {
					return this._originalFields[field] && this.fields[field] !== this._originalFields[field];
				});
				if (!needsPurged) { return Promise.resolve(); }
				// Need to purge the unclean
				return client.delete({
					type: this.model.getName(),
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
					type: this.model.getName(),
					id: this.fields.id,
					index: this.fields.index || index.getName(),
					parent: this.fields.parent,
					routing: this.fields.routing,
					body: esData
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
		let client;
		return this.model.connection.getClient()
			.then((_client) => {
				client = _client;
			})
			.then(() => this.model.trigger('pre-remove'))
			.then(() => {
				// Perform the actual delete
				return client.delete({
					type: this.model.getName(),
					id: this._originalFields.id,
					index: this._originalFields.index,
					routing: this._originalFields.routing,
					parent: this._originalFields.parent
				});
			})
			.then(() => this.model.trigger('post-remove'))
			.then(() => {
				// Update the data fields
				this._originalData = {};
				this._originalFields = {};
			})
			.then(() => this);
	}

}

module.exports = exports = ElasticsearchDocument;
