const XError = require('xerror');
const objtools = require('zs-objtools');

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
 * @method schemaToMapping
 * @throws {XError} - On invalid schema or unable to convert to ES mapping
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
 * @return {Object} - The ElasticSearch mapping
 */
function schemaToMapping(schema, extraIndexes=[], options={}) {
	let schemaData = schema.getData();
	if (schemaData.type !== 'object') {
		throw new XError('Schema root must be type "object" to be converted to ElasticSearch Mapping');
	}
	options.extraIndexes = extraIndexes;
	let [ mapping, idField ] = convertSubschema(schemaData, '', '', options);
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

// "Value" schema types hold data relating to their type
const VALUE_SCHEMA_TYPES = [ 'string', 'number', 'boolean', 'date', 'geopoint' ];
// "Control" schema types deal with holding other values
const CONTROL_SCHEMA_TYPES = [ 'object', 'array', 'map', 'or', 'mixed' ];
// Valid "nubmer" types
const NUMBER_TYPES = [ 'byte', 'short', 'integer', 'long', 'float', 'double' ];

// Map a Common Schema Index to an ElasticSearch Index
function commonIndexToESIndex(commonIndex, type) {
	if (type === 'string') {
		if (commonIndex === true) {
			return 'not_analyzed';
		} else if (commonIndex === false) {
			return undefined;
		} else if (commonIndex === 'analyzed') {
			return 'analyzed';
		}
	} else if (commonIndex) {
		// If not a string, truthy index values, means to use default indexing
		return undefined;
	}
	// False/undefined/null index values mean to not index the field
	return 'no';
}

// Find all relevant default/field schemas for a field
function findSubschemaSchemas(subschema, path, extraIndexes) {
	let subschemaIndex = subschema.index;
	subschema.index = commonIndexToESIndex(subschema.index, subschema.type);
	let schemas = {
		defaultSchema: subschema,
		extraSchemas: []
	};


	let setExtraIndexAsDefault = false;
	for (let extraIndex of extraIndexes) {
		if (extraIndex.field === path) {
			extraIndex.index = commonIndexToESIndex(extraIndex.index, subschema.type);
			let extraSchema = objtools.merge({}, subschema, extraIndex);
			if (!extraIndex.name && subschemaIndex === undefined) {
				if (!setExtraIndexAsDefault) {
					setExtraIndexAsDefault = true;
					schemas.defaultSchema = extraSchema;
				} else {
					throw new XError(`Multiple extra schemas tried to register as the default schema for "${path}"`);
				}
			} else {
				schemas.extraSchemas.push(extraSchema);
			}
		}
	}
	return schemas;
}

// Convert subschema to a "Value" mapping
function convertSubschemaValue(schema, name, path, options) {
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
			throw new XError(`Number type at "${path}" is an invalid ElasticSearch Number Type`);
		}
		if (schema.precisionStep !== undefined) {
			mapping.precision_step = parseInt(schema.precisionStep); //eslint-disable-line camelcase
			if (isNaN(mapping.precision_step)) { //eslint-disable-line camelcase
				throw new XError(`Number precision step at "${path}" must be an integer or a parsable string`);
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
			mapping.type = 'double';
		}

	}
	mapping.null_value = schema.default; //eslint-disable-line camelcase
	if (schema.index === 'analyzed') {
		mapping.analyzer = schema.analyzer || options.analyzer;
		if (!mapping.analyzer) {
			throw new XError(`Analyzed value at "${path}" is missing analyzer value`);
		}
	}
	return mapping;
}

// Convert subschema to a "Control" mapping
function convertSubschemaControl(schema, name, path, options) {
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
				let [ fieldMapping, fieldIdField ] = convertSubschema(property, field, fieldPath, options);
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
			return convertSubschema(schema.elements, name, path, options);
		case 'map':
		case 'or':
		case 'mixed':
			if (schema.index !== 'no') {
				mapping.dynamic = true;
			} else {
				mapping.enabled = false;
			}
			break;
	}
	return [ mapping, idField ];
}

// Convert subschema to an ElasticSearch Mapping
function convertSubschema(schema, name, path, options) {
	let mapping, idField;

	// Get/set/convert indexes
	if (options.defaultIndex && schema.index === undefined) { schema.index = options.defaultIndex; }
	let { defaultSchema, extraSchemas } = findSubschemaSchemas(schema, path, options.extraIndexes);

	if (VALUE_SCHEMA_TYPES.indexOf(schema.type) >= 0) {
		// Check if this is an ID field
		if (schema.id) { idField = path; }

		// Get the default mapping
		mapping = convertSubschemaValue(defaultSchema, name, path, options);

		if (extraSchemas.length) {
			mapping.fields = {};
			for (let extraSchema of extraSchemas) {
				// Try to get a name for the multi-field
				let name = extraSchema.name || '' + extraSchema.analyzer || '' + extraSchema.index;
				if (mapping.fields[name]) {
					throw new XError(`Value mapping cannot have multiple subfields with the same name (${name})`);
				}
				mapping.fields[name] = convertSubschemaValue(extraSchema, name, path, options);
			}
		}

	} else if (CONTROL_SCHEMA_TYPES.indexOf(schema.type) >= 0) {
		[ mapping, idField ] = convertSubschemaControl(schema, name, path, options);

	} else {
		// Otherwise, mapping doesn't coorespond to anything
		throw new XError(`Cannot convert unknown schema type (${schema.type}) to ElasticSearch Mapping`);
	}

	// ES Mapping contains extra, optional fields to merge into the mapping
	if (schema.esMapping) {
		mapping = objtools.merge(mapping, schema.esMapping);
	}
	return [ mapping, idField ];
}

exports.schemaToMapping = schemaToMapping;
