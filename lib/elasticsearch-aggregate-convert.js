
/**
 * Converts a common-query aggregate spec to an ElasticSearch aggregation.
 *
 * @method convertAggregate
 * @param {Aggregate} aggregate - The common-query aggregate
 * @param {Schema} schema - The common-schema schema for the model
 * @return {Object} - The ElasticSearch aggregation spec
 */
function convertAggregate(aggregate, schema) {
	// See https://www.elastic.co/guide/en/elasticsearch/reference/master/search-aggregations.html
	// for documentation on ES aggregates.  Note that not all types of aggregates allowed by
	// common-query may be possible in ES.  In the case that an aggregate cannot be converted,
	// throw an exception.
}
exports.convertAggregate = convertAggregate;

/**
 * Convert the result from an aggregate (from ElasticSearch) into the common-query
 * aggregate result format.
 *
 * @method convertAggregateResult
 * @param {Object} aggregateResult - The results from ElasticSearch
 * @param {Aggregate} aggregate - The common-query aggregate
 * @param {Schema} schema - The common-schema schema
 * @return {Object} - The common-query aggregate result data
 */
function convertAggregateResult(aggregateResult, aggregate, schema) {
	// Note that you may need to add another parameter for metadata that could be
	// passed back from convertAggregate if it's helpful to maintain some state
	// to avoid duplicating logic.
}
exports.convertAggregateResult = convertAggregateResult;
