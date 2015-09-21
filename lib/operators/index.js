const QueryOperatorRelated = require('./related');

function registerOperators(queryFactory) {
	queryFactory.registerQueryOperator('$child', new QueryOperatorRelated('$child'));
	queryFactory.registerQueryOperator('$parent', new QueryOperatorRelated('$parent'));
}
exports.registerOperators = registerOperators;
