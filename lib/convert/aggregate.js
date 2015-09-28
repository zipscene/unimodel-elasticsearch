const _ = require('lodash');
const moment = require('moment');
const objtools = require('zs-objtools');
const { AggregateValidationError } = require('zs-common-query');

const ISO_STRING_FORMAT = 'YYYY-MM-DDTHH:mm:ss.sssZ';

/**
 * Converts a common-query aggregate spec to an ElasticSearch aggregation.
 *
 * @method convertAggregate
 * @throws {AggregateValidationError} - When the aggregate cannot be converted
 * @param {Aggregate} aggregate - The common-query aggregate
 * @return {Object} - The ElasticSearch aggregation spec
 */
function convertAggregate(aggregate) {
	// See https://www.elastic.co/guide/en/elasticsearch/reference/master/search-aggregations.html
	// for documentation on ES aggregates.  Note that not all types of aggregates allowed by
	// common-query may be possible in ES.  In the case that an aggregate cannot be converted,
	// throw an exception.
	let metrics = buildMetrics(aggregate);
	let buckets = buildBuckets(aggregate);

	if (_.isEmpty(buckets)) {
		// No buckets, just do a metrics query
		return metrics;
	}
	// Perform the metric aggregation on each bucket
	for (let field in buckets) {
		let bucket = buckets[field];
		bucket.aggs = objtools.deepCopy(metrics);
	}
	return buckets;
}
exports.convertAggregate = convertAggregate;

function buildBuckets(aggregate) {
	let aggr = aggregate.getData();
	if (_.isEmpty(aggr.groupBy)) { return null; }

	let factory = aggregate.getAggregateFactory();
	let groupByAggregateType = factory.getAggregateType('GroupByAggregateType');
	let buckets = {};
	for (let i = 0, len = aggr.groupBy.length; i < len; ++i ) {
		let group = aggr.groupBy[i];
		let key = `group_${i}`;
		let groupByType = groupByAggregateType.findGroupByType(group);
		let name = groupByType.getName();
		let field = group.field;
		// Handle each of the known "groupByTypes" differently
		if (name === 'field') {
			// Terms Aggregation
			buckets[key] = { terms: { field } };
		} else if (name === 'range') {
			if (_.isEmpty(group.ranges)) { continue; }
			let isDate = _.isDate(group.ranges[0].start || group.ranges[0].end);
			let ranges = _.map(group.ranges, (range) => {
				let r = {};
				if (range.start) {
					r.from = (isDate) ? range.start.toISOString() : range.start;
				}
				if (range.end) {
					r.to = (isDate) ? range.end.toISOString() : range.end;
				}
				return r;
			});
			if (isDate) {
				// Date Range Aggregation
				buckets[key] = {
					'date_range': {
						field,
						ranges,
						format: ISO_STRING_FORMAT
					}
				};
			} else {
				// Range Aggregation
				buckets[key] = {
					range: { field, ranges }
				};
			}
		} else if (name === 'interval') {
			if (isNaN(group.interval)) {
				// Date Histogram Aggregation
				buckets[key] = {
					'date_histogram': {
						field,
						interval: `${moment.duration(group.interval).as('seconds')}s`,
						'extended_bounds': (group.base !== undefined) ? {
							min: group.base.toISOString()
						} : undefined
					}
				};
			} else {
				// Histogram Aggregation
				buckets[key] = {
					histogram: {
						field,
						interval: group.interval,
						'extended_bounds': (group.base !== undefined) ? {
							min: group.base
						} : undefined
					}
				};
			}
		}	else if (name === 'time-component') {
			// Date Historgram (requires some post processing)
			buckets[key] = {
				'date_histogram': {
					field,
					interval: group.timeComponent
				}
			};
		}
	}
	return buckets;
}

function buildMetrics(aggregate) {
	let aggr = aggregate.getData();
	if (_.isEmpty(aggr.stats)) { return null; }
	// Create a stats "metric" aggregation for each field (we will pull out mask values in the result)
	let metrics = {};
	for (let field in aggr.stats) {
		metrics[field] = { stats: { field } };
	}
	return metrics;
}

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
function convertAggregateResult(aggregateResult/*, aggregate, schema*/) {
	// Note that you may need to add another parameter for metadata that could be
	// passed back from convertAggregate if it's helpful to maintain some state
	// to avoid duplicating logic.
	console.log(JSON.stringify(aggregateResult, null, 2));
}
exports.convertAggregateResult = convertAggregateResult;
