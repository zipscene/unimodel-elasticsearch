const _ = require('lodash');
const moment = require('moment');
const objtools = require('zs-objtools');
const { AggregateValidationError } = require('zs-common-query');
const XError = require('xerror');

const JODA_ISO_STRING_FORMAT = 'yyyy-MM-ddHH:mm:ss.SSS';

function toJodaFormat(date) {
	return date.toISOString().replace(/[TZ]/g, '');
}

function fromJodaFormat(jodaStr) {
	return moment(jodaStr, 'YYYY-MM-DDHH:mm:ss.sss').toDate();
}

function getGroupTypeName(group, aggregate) {
	let factory = aggregate.getAggregateFactory();
	let groupByAggregateType = factory.getAggregateType('GroupByAggregateType');
	return groupByAggregateType.findGroupByType(group).getName();
}

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

	let buckets = [];
	for (let group of aggr.groupBy) {
		let typeName = getGroupTypeName(group, aggregate);
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
					r.from = (isDate) ? toJodaFormat(range.start) : range.start;
				}
				if (range.end) {
					r.to = (isDate) ? toJodaFormat(range.end) : range.end;
				}
				return r;
			});
			if (isDate) {
				// Date Range Aggregation
				buckets.push({
					'date_range': {
						field,
						ranges,
						format: JODA_ISO_STRING_FORMAT
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
							min: toJodaFormat(group.base)
						} : undefined,
						format: JODA_ISO_STRING_FORMAT
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
			//TODO: fix time-component
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
	let result = _convertAggregateResult(aggregateResult, aggregate, 0);
	if (!_.isArray(result)) {
		// Delete keys property on non-grouped aggregates
		delete result.keys;
	}
	return result;
}
exports.convertAggregateResult = convertAggregateResult;

function _convertAggregateResult(aggregateResult, aggregate, groupByIdx = 0, bucketIdx) {
	if (!aggregateResult.buckets || !aggregateResult.buckets.length) {
		let result = convertMetrics(aggregateResult, aggregate);
		result.keys = [];
		if (aggregate.getData().total) {
			result.total = aggregateResult.doc_count; //eslint-disable-line camelcase
		}
		return result;
	}
	let results = [];
	for (let i = 0, len = aggregateResult.buckets.length; i < len; i++) {
		let bucketResult = aggregateResult.buckets[i];
		let result = _convertAggregateResult(bucketResult, aggregate, (groupByIdx + 1), i);
		if (!result) { continue; }
		let key = extractResultKey(bucketResult, aggregate, groupByIdx, i);
		if (key !== undefined && key !== null) {
			result.keys.unshift(key);
		}
		results.push(result);
	}
	return results;
}

function extractResultKey(aggregateResult, aggregate, groupByIdx, bucketIdx) {
	let aggrData = aggregate.getData();
	if (!aggrData.groupBy || !aggrData.groupBy[groupByIdx]) {
		// We have no extra hints at what the key should be
		return aggregateResult.key;
	}
	let group = aggrData.groupBy[groupByIdx];
	let typeName = getGroupTypeName(group, aggregate);
	if (typeName === 'field') {
		// Terms
		return aggregateResult.key;
	} else if (typeName === 'range') {
		// Range
		return bucketIdx;
	} else if (typeName === 'interval') {
		if (isNaN(group.interval)) {
			return fromJodaFormat(aggregateResult.key);
		} else {
			return parseInt(aggregateResult.key);
		}
	} else if (typeName === 'time-component') {
		//TODO: fix time-component
	} else {
		return aggregateResult.key;
	}
}

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
