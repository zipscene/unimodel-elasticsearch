/**
 * This converts a common-query query to an elasticsearch query.  It takes into account the
 * common-schema schema for the type.
 *
 * @method elasticsearchQueryConvert
 * @throws {QueryValidationError} - When query cannot be converted or does not match the schema
 * @param {Query} query - The common-query object
 * @param {Schema} schema - The common-schema object
 * @return {Object} - The raw elasticsearch query
 */
function elasticsearchQueryConvert(query, schema) {

	// This should function very similarly to how it does now, but will need some minor modifcations
	// to account for differences in the format of queries and schemas.

	// The existing zsapi code for this is at zs-api/lib/elasticsearch/query.js:commonQueryToESFilter()
	// Remember to handle cases like $elemMatch can only use nested objects and such ...

}

exports.elasticsearchQueryConvert = elasticsearchQueryConvert;

