// Copyright 2016 Zipscene, LLC
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

const { query, update, aggregate } = require('common-query');

// Reexport the new operators
const operators = require('./operators');
exports.operators = operators;

// Export local query factory and helpers
let queryFactory = new query.QueryFactory();
// Register the new operators
operators.registerOperators(queryFactory);
exports.queryFactory = queryFactory;
function createQuery(queryData, options) {
	return queryFactory.createQuery(queryData, options);
}
exports.createQuery = createQuery;

// Export local update factory and helpers
let updateFactory = new update.UpdateFactory();
exports.updateFactory = updateFactory;
function createUpdate(updateData, options) {
	return updateFactory.createUpdate(updateData, options);
}
exports.createUpdate = createUpdate;

// Export local aggregate factory and helper creator
let aggregateFactory = new aggregate.AggregateFactory();
exports.aggregateFactory = aggregateFactory;
function createAggregate(aggregateData, options) {
	return aggregateFactory.createAggregate(aggregateData, options);
}
exports.createAggregate = createAggregate;
