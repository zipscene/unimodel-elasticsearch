// Copyright 2016 Zipscene, LLC
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

const _ = require('lodash');
const moment = require('moment');
const objtools = require('objtools');
const { AggregateValidationError } = require('common-query');

const {
	JODA_ISO_STRING_FORMAT,
	toJodaFormat,
	fromJodaFormat
} = require('./joda');

/**
 * Converts a common-query aggregate spec to an ElasticSearch aggregation.
 *
 * @method convertAggregate
 * @static
 * @throws {AggregateValidationError} - When the aggregate cannot be converted.
 * @param {Aggregate} aggregate - The common-query aggregate.
 * @return {Object} - The ElasticSearch aggregation spec.
 */
function convertAggregate(aggregate) {
	// See https://www.elastic.co/guide/en/elasticsearch/reference/master/search-aggregations.html
	// for documentation on ES aggregates.  Note that not all types of aggregates allowed by
	// common-query may be possible in ES.  In the case that an aggregate cannot be converted,
	// throw an exception.
	let metrics = buildMetricAggregations(aggregate);
	let buckets = buildBucketAggregations(aggregate);

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
		current.aggregations = { aggregate: bucket };
		while (current.aggregations && current.aggregations.aggregate) {
			current = current.aggregations.aggregate;
		}
	}
	if (!_.isEmpty(metrics)) {
		current.aggregations = metrics;
	}
	return root.aggregations.aggregate;
}
exports.convertAggregate = convertAggregate;

/**
 * Convert the result from an aggregate (from ElasticSearch) into the common-query
 * aggregate result format.
 *
 * @method convertAggregateResult
 * @static
 * @param {Object} aggregateResult - The results from ElasticSearch.
 * @param {Aggregate} aggregate - The common-query aggregate.
 * @return {Object|Object[]} - The common-query aggregate result data.
 */
function convertAggregateResult(aggregateResult, aggregate) {
	let result = convertBucketResults(aggregateResult, aggregate);
	if (!_.isArray(result)) {
		delete result.keys;
	}
	return result;
}
exports.convertAggregateResult = convertAggregateResult;


/**
 * Convert between IntervalGroupByType time units to ElasticSearch time units
 *
 * @property TIME_UNIT_MAP
 * @type Object
 * @private
 * @static
 */
const TIME_UNIT_MAP = {
	year: 'Y',
	month: 'M',
	week: 'w',
	day: 'd',
	hour: 'h',
	minute: 'm',
	second: 's'
};

/**
 * Get the GroupByType name for the given group.
 *
 * @method getGroupTypeName
 * @private
 * @static
 * @param {Object} group - Common-query aggregate groupBy data.
 * @param {Aggregate} aggregate - The common-query aggregate.
 * @return {String} The name of this group's type.
 */
function getGroupTypeName(group, aggregate) {
	let factory = aggregate.getAggregateFactory();
	let groupByAggregateType = factory.getAggregateType('GroupByAggregateType');
	return groupByAggregateType.findGroupByType(group).getName();
}

/**
 * Build ElasticSearch aggregation buckets out of GroupBy types.
 *
 * @method buildBucketAggregations
 * @private
 * @static
 * @throws {AggregateValidationError} If the aggregate contains invalid data.
 * @param {Aggregate} aggregate - The common-query aggregate.
 * @return {Object[]} The aggregation buckets.
 */
function buildBucketAggregations(aggregate) {
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
			// Convert ranges based on if they are date objects
			let isDate = _.isDate(group.ranges[0].start || group.ranges[0].end);
			let ranges = [];
			for (let range of group.ranges) {
				let r = {};
				if (range.start) {
					r.from = (isDate) ? toJodaFormat(range.start) : range.start;
				}
				if (range.end) {
					r.to = (isDate) ? toJodaFormat(range.end) : range.end;
				}
				ranges.push(r);
			}
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
			if (group.base !== null && group.base !== undefined) {
				throw new AggregateValidationError('Interval base is not supported for ElasticSearch aggregates.');
			}
			if (isNaN(group.interval)) {
				// Date Histogram
				buckets.push({
					'date_histogram': {
						field,
						interval: `${moment.duration(group.interval).as('seconds')}s`
					}
				});
			} else {
				// Histogram
				buckets.push({
					histogram: {
						field,
						interval: group.interval
					}
				});
			}

		}	else if (typeName === 'time-component') {
			// Date Historgram
			let unit = TIME_UNIT_MAP[group.timeComponent];
			if (!unit) {
				let msg = 'Could not convert time component into a valid ElasticSearch Time Unit';
				throw new AggregateValidationError(msg);
			}
			let interval = (isNaN(group.timeComponentCount)) ? 1 : group.timeComponentCount;
			buckets.push({
				'date_histogram': {
					field,
					interval: `${interval}${unit}`
				}
			});
		}
	}
	return buckets;
}

/**
 * Build Metric Aggrations based on "stats" aggregation type.
 *
 * @method buildMetricAggregations
 * @private
 * @static
 * @param {Aggregate} aggregate - The common-query aggregate.
 * @return {Object} The metric aggregation spec.
 */
function buildMetricAggregations(aggregate) {
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
 * Recursively convert bucketed results, building up the keys array and stripping out leafs with no children.
 *
 * @method convertBucketResults
 * @private
 * @static
 * @param {Object} bucketResult - The current bucket node.
 * @param {Aggregate} aggregate - The common-query aggregate.
 * @param {Number} [groupByIdx=-1] - The current index into the "groupBy" array in the aggregate.
 * @param {Number} [bucketIdx=-1] - The index into the parent node's "buckets" array.
 * @return {Object|Object[]} The current common-query aggregate result data.
 */
function convertBucketResults(bucketResult, aggregate, groupByIdx = -1, bucketIdx = -1) {
	let buckets = bucketResult.buckets || bucketResult.aggregate && bucketResult.aggregate.buckets;
	if (!buckets || !buckets.length) {
		// If there are no buckets, this is considered a "metric" result
		return convertMetricResults(bucketResult, aggregate, groupByIdx, bucketIdx);
	}
	let key = extractGroupKey(bucketResult, aggregate, groupByIdx, bucketIdx);
	let result = [];
	for (let i = 0, len = buckets.length; i < len; i++) {
		let bucket = buckets[i];
		if (bucket.doc_count <= 0) { //eslint-disable-line camelcase
			// Ignore leaves without any children (they show up because of multiple groupBy cluases)
			continue;
		}

		// Convert child
		let groups = convertBucketResults(bucket, aggregate, (groupByIdx + 1), i);
		if (!_.isArray(groups)) { groups = [ groups ]; }

		// Add key to the front of the keys array, if the key exists
		if (key !== null && key !== undefined) {
			for (let group of groups) {
				group.keys.unshift(key);
			}
		}

		// Push all results
		result.push(...groups);
	}
	return result;
}

/**
 * Convert a bucketed result with "metrics".
 * This pulls off stats, totals, and keys from the current result object.
 *
 * @method convertMetricResults
 * @private
 * @static
 * @param {Object} metricResults - The leaf node containing metrics.
 * @param {Aggregate} aggregate - The common-query aggregate.
 * @param {Number} [groupByIdx=-1] - The current index into the "groupBy" array in the aggregate.
 * @param {Number} [bucketIdx=-1] - The index into the parent node's "buckets" array.
 * @return {Object} The "metric" leaf node as a common-query aggregate result.
 */
function convertMetricResults(metricResults, aggregate, groupByIdx = -1, bucketIdx = -1) {
	let aggrData = aggregate.getData();
	let result = {};

	// Build stats
	let stats = {};
	for (let key in metricResults) {
		let metricResult = metricResults[key];
		if (_.isEmpty(metricResult)) { continue; }
		let field = key.substring(1);
		let fieldStats = objtools.getPath(aggrData, `stats.${field}`);
		if (_.isEmpty(fieldStats)) { continue; }
		let fieldResult = {};
		for (let stat in metricResult) {
			if (!fieldStats[stat]) { continue; }
			fieldResult[stat] = metricResult[stat];
		}
		stats[field] = fieldResult;
	}
	if (!_.isEmpty(stats)) { result.stats = stats; }

	if (aggrData.total) {
		// If we want the total, grab the full doc count on this result
		result.total = metricResults.doc_count; //eslint-disable-line camelcase
	}
	// Gab a key associated with this data (used in groupBy aggregates)
	let key = extractGroupKey(metricResults, aggregate, groupByIdx, bucketIdx);
	result.keys = [ key ];

	return result;

}

/**
 * Extract a "Group Key" from a bucket result.
 *
 * @method extractResultKey
 * @private
 * @static
 * @param {Object} bucketResult - Current bucket result node to get key for.
 * @param {Aggregate} aggregate - Common-query aggregate.
 * @param {Number} [groupByIdx=-1] - The current index into the "groupBy" array in the aggregate.
 * @param {Number} [bucketIdx=-1] - The index into the parent node's "buckets" array.
 * @return {String|Number|Null} The "group key" for this bucket result
 */
function extractGroupKey(bucketResult, aggregate, groupByIdx = -1, bucketIdx = -1) {
	let aggrData = aggregate.getData();
	if (!aggrData.groupBy || !aggrData.groupBy[groupByIdx]) {
		// This should be a "metric" result with no groupBy entries
		return null;
	}
	let group = aggrData.groupBy[groupByIdx];
	let typeName = getGroupTypeName(group, aggregate);
	if (typeName === 'range') {
		// Date/Numeric Range
		return bucketIdx;
	} else if (typeName === 'interval') {
		if (isNaN(group.interval)) {
			// Date Interval
			return fromJodaFormat(bucketResult.key_as_string);
		} else {
			// Numeric Interval
			return parseInt(bucketResult.key);
		}
	} else if (typeName === 'time-component') {
		// Time Component
		return fromJodaFormat(bucketResult.key_as_string);
	} else {
		// Field/unknown
		return bucketResult.key;
	}
}
