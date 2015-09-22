const QueryOperatorParent = require('./parent');
const QueryOperatorChild = require('./child');

function registerOperators(queryFactory) {
	queryFactory.registerQueryOperator('$child', new QueryOperatorChild());
	queryFactory.registerQueryOperator('$parent', new QueryOperatorParent());
}
exports.registerOperators = registerOperators;
