const _ = require('lodash');
const moment = require('moment');
const objtools = require('zs-objtools');
const { AggregateValidationError } = require('zs-common-query');
const XError = require('xerror');

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
		// Return global aggregation for the metrics
		let aggr = { global: {} };
		if (!_.isEmpty(metrics)) {
			aggr.aggregations = metrics;
		}
		return aggr;
	}

	// Build recursive aggregates
	let current = {};
	let root = current;
	for (let bucket of buckets) {
		current.aggregations = bucket;
		current = bucket;
	}
	if (!_.isEmpty(metrics)) {
		current.aggregations = metrics;
	}
	return root.aggregations;
}
exports.convertAggregate = convertAggregate;

function buildBuckets(aggregate) {
	let aggr = aggregate.getData();
	if (_.isEmpty(aggr.groupBy)) { return []; }

	let factory = aggregate.getAggregateFactory();
	let groupByAggregateType = factory.getAggregateType('GroupByAggregateType');
	let buckets = [];
	for (let group of aggr.groupBy) {
		let typeName = groupByAggregateType.findGroupByType(group).getName();
		let field = group.field;
		// Handle each of the known "groupByTypes" differently
		if (typeName === 'field') {
			// Terms Aggregation
			buckets.push({ terms: { field } });
		} else if (typeName === 'range') {
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
				buckets.push({
					'date_range': {
						field,
						ranges,
						format: ISO_STRING_FORMAT
					}
				});
			} else {
				// Range Aggregation
				buckets.push({
					range: { field, ranges }
				});
			}
		} else if (typeName === 'interval') {
			if (isNaN(group.interval)) {
				// Date Histogram Aggregation
				buckets.push({
					'date_histogram': {
						field,
						interval: `${moment.duration(group.interval).as('seconds')}s`,
						'extended_bounds': (group.base !== undefined) ? {
							min: group.base.toISOString()
						} : undefined
					}
				});
			} else {
				// Histogram Aggregation
				buckets.push({
					histogram: {
						field,
						interval: group.interval,
						'extended_bounds': (group.base !== undefined) ? {
							min: group.base
						} : undefined
					}
				});
			}
		}	else if (typeName === 'time-component') {
			// Date Historgram (requires some post processing)
			buckets.push({
				'date_histogram': {
					field,
					interval: group.timeComponent
				}
			});
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
		metrics[`|${field}`] = { stats: { field } };
	}
	return metrics;
}

/**
 * Convert the result from an aggregate (from ElasticSearch) into the common-query
 * aggregate result format.
 *
 * @method convertAggregateResult
 * @param {Object} aggregateResult - The results from ElasticSearch.
 * @param {Aggregate} aggregate - The common-query aggregate.
 * @param {Schema} schema - The common-schema schema.
 * @return {Object} - The common-query aggregate result data.
 */
function convertAggregateResult(aggregateResult, aggregate) {
	if (!aggregateResult.buckets || !aggregateResult.buckets.length) {
		let result = convertMetrics(aggregateResult, aggregate);
		if (aggregateResult.key) {
			result.keys = [ aggregateResult.key ];
		}
		if (aggregate.getData().total) {
			result.total = aggregateResult.doc_count; //eslint-disable-line camelcase
		}
		return result;
	}
	let results = [];
	for (let bucket of aggregateResult.buckets) {
		let result = convertAggregateResult(bucket, aggregate);
		if (!result) { continue; }
		if (aggregateResult.key) {
			result.keys.unshift(aggregateResult.key);
		}
		results.push(result);
	}
	return results;
}
exports.convertAggregateResult = convertAggregateResult;


function convertMetrics(metricResults, aggregate) {
	let stats = {};
	let aggr = aggregate.getData();
	for (let key in metricResults) {
		let metricResult = metricResults[key];
		if (_.isEmpty(metricResult)) { continue; }
		let field = key.substring(1);
		let fieldStats = objtools.getPath(aggr, `stats.${field}`);
		if (_.isEmpty(fieldStats)) { continue; }
		let result = {};
		for (let stat in metricResult) {
			if (!fieldStats[stat]) { continue; }
			result[stat] = metricResult[stat];
		}
		stats[field] = result;
	}
	if (_.isEmpty(stats)) { return {}; }
	return { stats };
}
