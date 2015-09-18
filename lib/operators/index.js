const QueryOperatorRelated = require('./related');

function registerQueryOperators(commonQueryFactory) {
	commonQueryFactory.registerQueryOperator('$child', new QueryOperatorRelated('$child'));
	commonQueryFactory.registerQueryOperator('$parent', new QueryOperatorRelated('$parent'));
}
exports.registerQueryOperators = registerQueryOperators;
