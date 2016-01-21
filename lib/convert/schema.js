const objtools = require('zs-objtools');
const { createSchema } = require('zs-common-schema');

const ElasticsearchMappingValidationError = require('../elasticsearch-mapping-validation-error');

/**
 * Converts a common-schema schema to an ElasticSearch mapping.  The schema is specified in
 * common-schema form, with these additions:
 *
 * - Fields declared as 'number' can contain an additional property, `numberType`.  This can
 *   contain the ElasticSearch mapping values for the different types of numbers, such as
 *   `integer`, `short`, etc.  Fields without this property should be treated as a double.
 * - Fields can contain an `index` property  which maps to the `index` property on the mapping.
 *   A value of `true` maps to `not_analyzed`.  A value of `false`, `undefined`, or `null` maps
 *   to `no`.  A value of `analyzed` maps to `analyzed`, and the field must also contain an
 *   `analyzer` property.  If no index property is specified, it defaults to `no`.
 * - Fields can contain an `analyzer` property if their `index` property is set to `analyzed`.
 *   This maps directly to the ES `analyzer` property in the mapping.
 * - Fields can contain an `esMapping` property which is an object containing additional
 *   properties to merge into the mapping for that field.
 * - Fields of type `object` can contain a `nested` property.  When set to true, this causes
 *   the object to be stored as a nested object.
 *
 *
 * @method convertSchema
 * @throws {ElasticsearchMappingValidationError} - On invalid schema or unable to convert to ES mapping
 * @param {Schema} schema - The common-schema Schema object to convert.
 * @param {Object[]} extraIndexes - An array of extra indexes to add to the mapping in addition
 *   to those defined in the schema.  This should be in the format:
 *   [
 *     { field: 'foo', index: 'not_analyzed', name: 'raw' },
 *     { field: 'bar', index: 'analyzed', analyzer: 'english' }
 *   ]
 *   The `name` property is used to construct multi-fields wherein one field can be indexed in
 *   multiple different ways.  It is mapped into the `fields` property on a field in the mapping.
 *   See https://www.elastic.co/guide/en/elasticsearch/reference/current/_multi_fields.html .  If
 *   multiple indexes are specified for the same field with the same `name` (or with no name, as
 *   the default) it triggers an error.
 * @param {Object} [options={}]
 *   @param {String} [options.parentType] - If this is a child type, the type name of the parent.
 *   @param {Boolean} [options.includeAllField] - If this is true, the _all field will be enabled in the mapping.
 * @return {Object} - The ElasticSearch mapping
 */
function convertSchema(schema, extraIndexes=[], options={}) {
	let schemaData = objtools.deepCopy(schema.getData());
	if (schemaData.type !== 'object') {
		let msg = 'Schema root must be type "object" to be converted to ElasticSearch Mapping';
		throw new ElasticsearchMappingValidationError(msg);
	}
	options.extraIndexes = extraIndexes;
	let [ mapping, idField ] = convertSubschema(schemaData, '', '', extraIndexes);
	// Remove fields which shouldn't be set on the root object
	delete mapping.type;
	delete mapping.dynamic;
	if (idField) {
		// Set the _id field if an ID field wa found in the schema
		mapping._id = mapping._id || {};
		mapping._id = objtools.merge(mapping._id, { path: idField });
	}
	if (options.parentType) {
		mapping._parent = { type: options.parentType };
	}
	mapping._all = { enabled: (options.includeAllField === true) };
	return mapping;
}

exports.convertSchema = convertSchema;

// Common Schema "Value" types (hold values relating to their type).
const VALUE_SCHEMA_TYPES = [ 'string', 'number', 'boolean', 'date', 'geopoint' ];
// Common Schema "Control" types (deal with holding other values).
const CONTROL_SCHEMA_TYPES = [ 'object', 'array', 'map', 'or', 'mixed' ];
// ElasticSearch "number" types.
const NUMBER_TYPES = [ 'byte', 'short', 'integer', 'long', 'float', 'double' ];

/**
 * Map a Common Schema Index to an ElasticSearch Index.
 *
 * @method commonIndexToESIndex
 * @private
 * @param {String} commonIndex - Value of the index field in a Common Schema object.
 * @param {String} type - Common Schema type field.
 * @return {String|Undefiend} ElasticSearch Index type ('no', 'analyzed', 'not_analyzed', or undefined)
 */
function commonIndexToESIndex(commonIndex, type) {
	if (type === 'string') {
		if (commonIndex === true || commonIndex === 'not_analyzed') {
			return 'not_analyzed';
		} else if (commonIndex === 'analyzed') {
			return 'analyzed';
		}
	} else if (commonIndex) {
		// If not a string, truthy index values, means to use default behavior (ie don't not index)
		return undefined;
	}
	// Default behavior should be to not index (unlike default ES behavior)
	return 'no';
}

/**
 * Given a subschema, and the list of extraIndexes, get the default schema, and a list of multifield
 * schemas to put in the `fields` property of a Value mapping.
 *
 * @method findSubschemaSchemas
 * @private
 * @throws {ElasticsearchMappingValidationError} - If multiple extra indexes try to register as the default schema.
 * @param {Object} subschema - Schema data to build all schemas off.
 * @param {String} path - Path to this schema in the root schema.
 * @param {Object[]} [extraIndexes] - Extra indexes to build into multifield schemas.
 *   @param {String} extraIndexes.field - Field path this index relates to.
 *   @param {Mixed} extraIndexes.index - Type of index that should be applied under the multifield.
 *   @param {String} [extraIndexes.name=analyzer || index] - If undefined, this extra index will try
 *     to become the defaultSchema.
 *   @param {String} [extraIndexes.analyzer] - If index is 'analyzed', this is the analzyer to use.
 * @return {Object} - Containing the defaultSchema (used as the field's mapping)
 *   and extraSchemas (which will be added as multifield mappings).
 */
function findSubschemaSchemas(subschema, path, extraIndexes) {
	// Since arrays are not an explicite type in ES, we can't use subschemas of type "array" directly. Instead, we use
	// the "elements" field of this "array" subschema to determine the data type. The "array" subschema may include an
	// "index" field that we need to save to the elements field.
	if (subschema.type === 'array' && subschema.elements && typeof subschema.elements.index === 'undefined') {
		subschema.elements.index = subschema.index;
	}
	let subschemaIndex = subschema.index;
	subschema.index = commonIndexToESIndex(subschema.index, subschema.type);
	let defaultSchema = subschema;
	let extraSchemas = [];

	let matchingExtraIndexes = [];
	let setExtraIndexAsDefault = false;
	for (let extraIndex of extraIndexes) {
		if (extraIndex.field === path) {
			matchingExtraIndexes.push(extraIndex);
		}
	}
	if (matchingExtraIndexes.length && VALUE_SCHEMA_TYPES.indexOf(subschema.type) < 0) {
		let msg = `Cannot apply extra index to a non-"value" schema type`;
		throw new ElasticsearchMappingValidationError(msg);
	}
	for (let extraIndex of matchingExtraIndexes) {
		extraIndex.index = commonIndexToESIndex(extraIndex.index, subschema.type);
		let extraSchema = objtools.merge({}, subschema, extraIndex);
		if (!extraIndex.name && subschemaIndex === undefined) {
			if (!setExtraIndexAsDefault) {
				setExtraIndexAsDefault = true;
				defaultSchema = extraSchema;
			} else {
				let msg = `Multiple extra schemas tried to register as the default schema for "${path}"`;
				throw new ElasticsearchMappingValidationError(msg);
			}
		} else {
			extraSchemas.push(extraSchema);
		}
	}
	return { defaultSchema, extraSchemas };
}

/**
 * Convert subschema to a "Value" mapping
 * A "Value" mapping is a subschema type that holds/indexes the data it is associated with in the mapping.
 *
 * @method convertSubschemaValue
 * @private
 * @throws {ElasticsearchMappingValidationError} - If schema data is invalid.
 * @param {Object} schema - Schema data for this value field.
 * @param {String} name - Child name of the property this schema maps to in its parent.
 * @param {String} path - Path to this schema in the root schema.
 * @return {Object} The ElasticSearch mapping for the Value schema.
 */
function convertSubschemaValue(schema, name, path) {
	// Default mapping object
	let mapping = {
		type: schema.type,
		index: schema.index
	};

	// Some types needs to be converted
	if (schema.type === 'number') {
		// Extra number type options
		mapping.type = schema.numberType  || 'double';
		if (NUMBER_TYPES.indexOf(mapping.type) < 0) {
			let msg = `Number type at "${path}" is an invalid ElasticSearch Number Type`;
			throw new ElasticsearchMappingValidationError(msg);
		}
		if (schema.precisionStep !== undefined) {
			mapping.precision_step = parseInt(schema.precisionStep); //eslint-disable-line camelcase
			if (isNaN(mapping.precision_step)) { //eslint-disable-line camelcase
				let msg = `Number precision step at "${path}" must be an integer or a parsable string`;
				throw new ElasticsearchMappingValidationError(msg);
			}
		} else {
			mapping.precision_step = 8; //eslint-disable-line camelcase
		}

	} else if (schema.type === 'date') {
		mapping.format = schema.format || 'date_optional_time';

	} else if (schema.type === 'geopoint') {
		if (schema.index !== 'no') {
			// We are indexing this
			mapping.type = 'geo_point';
			mapping.geohash = schema.geohash !== false;
		} else {
			// If not indexing, assume it's a GeoJSON [ long, lat ] array
			// Must be type double, since geo_point is always indexed
			mapping.type = 'double';
		}

	}
	mapping.null_value = schema.default; //eslint-disable-line camelcase
	if (schema.index === 'analyzed') {
		mapping.analyzer = schema.analyzer;
		if (!mapping.analyzer) {
			let msg = `Analyzed value at "${path}" is missing analyzer value`;
			throw new ElasticsearchMappingValidationError(msg);
		}
	}
	return mapping;
}

/**
 * Returns true if any fields underneath the given schema are indexed.
 *
 * @method hasSubfieldsIndexed
 * @param {Object} schema - Schema data
 * @return {Boolean} - Whether or not subfields are indexed
 */
function hasSubfieldsIndexed(schema) {
	let hasIndex = false;
	createSchema(schema).traverseSchema({
		onSubschema(subschema) {
			if (subschema.index !== 'no') {
				hasIndex = true;
				return false;
			}
		}
	});
	return hasIndex;
}

/**
 * Convert subschema to a "Control" mapping.
 * A "Control" mapping is a subschema type that holds other "Value"/"Control" mappings.
 *
 * @method convertSubschemaControl
 * @private
 * @throws {ElasticsearchMappingValidationError} - If the schema type is invalid or subschemas are invalid.
 * @param {Object} schema - Schema data for this value field.
 * @param {String} name - Child name of the property this schema maps to in its parent.
 * @param {String} path - Path to this schema in the root schema.
 * @param {Object[]} [extraIndexes] - Extra indexes to apply to fields (as multifields)
 *   @param {String} extraIndexes.field - Field path this index relates to.
 *   @param {Mixed} extraIndexes.index - Type of index that should be applied under the multifield.
 *   @param {String} [extraIndexes.name=analyzer || index] - If undefined, this extra index will try
 *     to become the defaultSchema.
 *   @param {String} [extraIndexes.analyzer] - If index is 'analyzed', this is the analzyer to use.
 * @return {Object} The ElasticSearch mapping.
 */
function convertSubschemaControl(schema, name, path, extraIndexes) {
	let mapping = {
		type: 'object',
		dynamic: false
	};
	let idField;

	switch (schema.type) {
		case 'object':
			mapping.properties = {};
			for (let field in schema.properties) {
				let property = schema.properties[field];
				let fieldPath = path ? `${path}.${field}` : field;
				let [ fieldMapping, fieldIdField ] = convertSubschema(property, field, fieldPath, extraIndexes);
				mapping.properties[field] = fieldMapping;
				idField = fieldIdField || idField;
			}
			if (schema.nested) {
				// Set type to nested, and include nesting options
				mapping.type = 'nested';
				if (schema.includeInParent) { mapping.include_in_parent = true; } //eslint-disable-line camelcase
				if (schema.includeInRoot !== false) { mapping.include_in_root = true; } //eslint-disable-line camelcase
			}
			break;
		case 'array':
			return convertSubschema(schema.elements, name, path, extraIndexes);
		case 'mixed':
			if (schema.index !== 'no') {
				mapping.dynamic = true;
			} else {
				mapping.enabled = false;
			}
			break;
		case 'map':
			if (hasSubfieldsIndexed(schema)) {
				mapping.dynamic = true;
			} else {
				mapping.enabled = false;
			}
			break;
		case 'or':
			// These types do not map cleanly to the ElasticSearch model (indexing will not work correctly)
			let msg = `Invalid schema type provided: ${schema.type}`;
			throw new ElasticsearchMappingValidationError(msg);
	}
	return [ mapping, idField ];
}

/**
 * Convert the given schema at the name/path into a valid ElasticSearch Mapping
 *
 * @method convertSubschema
 * @private
 * @throws {ElasticsearchMappingValidationError} - If the schema type is invalid or subschemas are invalid.
 * @param {Object} schema - Schema data for this value field.
 * @param {String} name - Child name of the property this schema maps to in its parent.
 * @param {String} path - Path to this schema in the root schema.
 * @param {Object[]} [extraIndexes] - Extra indexes to apply to fields (as multifields)
 *   @param {String} extraIndexes.field - Field path this index relates to.
 *   @param {Mixed} extraIndexes.index - Type of index that should be applied under the multifield.
 *   @param {String} [extraIndexes.name=analyzer || index] - If undefined, this extra index will try
 *     to become the defaultSchema.
 *   @param {String} [extraIndexes.analyzer] - If index is 'analyzed', this is the analzyer to use.
 * @return {Object} The ElasticSearch mapping.
 */
function convertSubschema(schema, name, path, extraIndexes) {
	let mapping, idField;

	// Get/set/convert to schemas and extraSchemas
	let { defaultSchema, extraSchemas } = findSubschemaSchemas(schema, path, extraIndexes);

	if (VALUE_SCHEMA_TYPES.indexOf(schema.type) >= 0) {
		// Check if this is an ID field
		if (schema.id) { idField = path; }

		// Get the default mapping
		mapping = convertSubschemaValue(defaultSchema, name, path);

		if (extraSchemas.length) {
			mapping.fields = {};
			for (let extraSchema of extraSchemas) {
				// Try to get a name for the multi-field
				let name = extraSchema.name || '' + extraSchema.analyzer || '' + extraSchema.index;
				if (mapping.fields[name]) {
					let msg = `Value mapping cannot have multiple subfields with the same name (${name})`;
					throw new ElasticsearchMappingValidationError(msg);
				}
				mapping.fields[name] = convertSubschemaValue(extraSchema, name, path);
			}
		}

	} else if (CONTROL_SCHEMA_TYPES.indexOf(schema.type) >= 0) {
		[ mapping, idField ] = convertSubschemaControl(defaultSchema, name, path, extraIndexes);

	} else {
		// Otherwise, mapping doesn't coorespond to anything
		let msg = `Cannot convert unknown schema type (${schema.type}) to ElasticSearch Mapping`;
		throw new ElasticsearchMappingValidationError(msg);
	}

	// ES Mapping contains extra, optional fields to merge into the mapping
	if (schema.esMapping) {
		mapping = objtools.merge(mapping, schema.esMapping);
	}
	return [ mapping, idField ];
}
