
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
		return this.fields._id;
	}

	/**
	 * Sets the internal _id field.
	 *
	 * @method setInternalId
	 * @param {String} id
	 */
	setInternalId(id) {
		this.fields._id = id;
	}

	/**
	 * Returns the internal _parent field.
	 *
	 * @method getParentId
	 * @return {String}
	 */
	getParentId() {
		return this.fields._parent;
	}

	/**
	 * Sets the internal _parent field.
	 *
	 * @method setParentId
	 * @param {String} parent
	 */
	setParentId(parent) {
		this.fields._parent = parent;
	}

	/**
	 * Returns the internal _routing field.
	 *
	 * @method getRouting
	 * @return {String}
	 */
	getRouting() {
		return this.fields._routing;
	}

	/**
	 * Sets the internal _routing field.
	 *
	 * @method setRouting
	 * @param {String} routing
	 */
	setRouting(routing) {
		this.fields._routing = routing;
	}

	/**
	 * Returns the internal _index field.
	 *
	 * @method getIndexId
	 * @return {String}
	 */
	getIndexId() {
		return this.fields._index;
	}

	/**
	 * Sets the internal _index field.
	 *
	 * @method setIndexId
	 * @param {String} index
	 */
	setIndexId(index) {
		this.fields._index = index;
	}

	save() {
		// - Normalize data according to the schema
		// - Call pre-normalize, post-normalize, and pre-save hooks
		// - If this was an existing document (this._originalFields exists) and any of _id, _routing,
		//   _index, _parent fields have changed, then the old document must be removed first.
		// - Execute an `index` call on ElasticSearch to save the new document data.
		// - Set this._originalData and this._originalFields to deep-copied versions of this.data and this.fields
		// - Set the values of this.fields and this._originalFields to the values updated from the server (ie, the server will generate
		//   an _id) or at least from known values (ie, we already know the _index it was saved to)
		// - Execute the post-save hook
	}

	remove() {
		// - Remember to use this._originalFields instead of this.fields to get the id, index, and routing to remove
		// Execute pre/post remove hooks
		// Execute the ES remove document
	}

}
