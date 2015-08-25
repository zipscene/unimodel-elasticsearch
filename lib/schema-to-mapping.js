
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
function schemaToMapping(schema, extraIndexes, options={}) {

	// This should do what it says on the tin, specified by the function above.
	// The existing (current zsapi) code should be able to be largely re-used for this, but will
	// require some changes to conform to the slightly modified spec above, and to the slightly
	// modified schema format.

	// The current zsapi code for this lives in: zs-api/lib/elasticsearch/schema.js:schemaToMapping()
	// If there are any features or options supported in the existing code that aren't mentioned in
	// the new spec, ask me.

	// Additional reference documentation that may be helpful (I recommend reading this first):
	// https://www.elastic.co/guide/en/elasticsearch/guide/current/mapping-intro.html
	// https://www.elastic.co/guide/en/elasticsearch/reference/current/_multi_fields.html

	// Regarding multifields:
	// The `index` property specified in the schema is always the default index value for the field.
	// If additional indexes are passed in via `extraIndexes`, these can be merged in as multifields.
	// The only exception to this is when the `index` property in the schema is undefined, and the
	// object passed into `extraIndexes` does not have a `name` property.  In this case, the object
	// passed into `extraIndexes` becomes the default index.

}

exports.schemaToMapping = schemaToMapping;
