const _ = require('lodash');
const moment = require('moment');
const objtools = require('zs-objtools');
const { AggregateValidationError } = require('zs-common-query');

const {
	JODA_ISO_STRING_FORMAT,
	toJodaFormat,
	fromJodaFormat
} = require('./joda');


const TIME_UNIT_MAP = {
	year: 'Y',
	month: 'M',
	week: 'w',
	day: 'd',
	hour: 'h',
	minute: 'm',
	second: 's'
};


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
		current.aggregations = { aggregate: bucket };
		current = bucket;
	}
	if (!_.isEmpty(metrics)) {
		current.aggregations = metrics;
	}
	return root.aggregations.aggregate;
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
	let result = convertBucketResults(aggregateResult, aggregate);
	if (!_.isArray(result)) {
		delete result.keys;
	}
	return result;
}
exports.convertAggregateResult = convertAggregateResult;

function convertBucketResults(bucketResult, aggregate, groupByIdx = -1, bucketIdx = -1) {
	let buckets = bucketResult.buckets || bucketResult.aggregate && bucketResult.aggregate.buckets;
	if (!buckets || !buckets.length) {
		return convertMetricResults(bucketResult, aggregate, groupByIdx, bucketIdx);
	}
	let key = extractResultKey(bucketResult, aggregate, groupByIdx, bucketIdx);
	let result = [];
	for (let i = 0, len = buckets.length; i < len; i++) {
		let bucket = buckets[i];
		if (bucket.doc_count <= 0) { continue; } //eslint-disable-line camelcase
		let groups = convertBucketResults(bucket, aggregate, (groupByIdx + 1), i);
		if (!_.isArray(groups)) { groups = [ groups ]; }

		if (key !== null && key !== undefined) {
			for (let group of groups) {
				group.keys.unshift(key);
			}
		}

		result.push(...groups);
	}
	return result;
}

function convertMetricResults(metricResults, aggregate, groupByIdx, bucketIdx) {
	let aggrData = aggregate.getData();

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

	let result = {};
	if (!_.isEmpty(stats)) { result.stats = stats; }
	if (aggrData.total) {
		result.total = metricResults.doc_count; //eslint-disable-line camelcase
	}
	let key = extractResultKey(metricResults, aggregate, groupByIdx, bucketIdx);
	result.keys = [ key ];
	return result;

}

function extractResultKey(aggregateResult, aggregate, groupByIdx, bucketIdx) {
	let aggrData = aggregate.getData();
	if (!aggrData.groupBy || !aggrData.groupBy[groupByIdx]) {
		// We have no extra hints at what the key should be
		return null;
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
			return fromJodaFormat(aggregateResult.key_as_string);
		} else {
			return parseInt(aggregateResult.key);
		}
	} else if (typeName === 'time-component') {
		return fromJodaFormat(aggregateResult.key_as_string);
	} else {
		return aggregateResult.key;
	}
}
