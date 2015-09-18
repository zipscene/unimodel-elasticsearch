const _ = require('lodash');
const {
	QueryOperator,
	QueryValidationError
} = require('zs-common-query');

/**
 * The $parent operator, which is only implemented in ElasticSearch
 */
class QueryOperatorRelated extends QueryOperator {

	constructor(name) {
		super(name || '$related');
	}

	matches(value, operatorValue, operator, options, query) {
		if (_.isPlainObject(operatorValue) || _.isEmpty(operatorValue)) { return false; }
		return _.every(operatorValue, (subquery, model) => {
			if (options.schema) {
				let RelatedModel = require('../index').model(model);
				options.schema = RelatedModel.getSchema();
			}
			query._matchSubquery(subquery, value, options);
		});
		//TODO: this function is just completely wrong right now
	}

	traverse(operatorValue, operator, query/*, handlers*/) {
		if (_.isPlainObject(operatorValue) && !_.isEmpty(operatorValue)) {
			for (let relative in operatorValue) {
				if (relative[0] === '$') { continue; }
				let subquery = operatorValue[relative];
				if (!_.isPlainObject(subquery)) { continue; }
				let Relative; //eslint-disable-line no-unused-vars
				try {
					Relative = require('../index').model(relative);
				} catch(err) {
					let msg = `Related model in ${operator} must be globally registered`;
					throw new QueryValidationError(msg, { query }, err);
				}
				//TODO: I'm stuck here, on how traversal works across the Child <-> Parent boundry
				// The traverse function knows nothing about "normalizing", nor the schema.
				// The schema seems necessary to actually traverse across the boundries
				// I don't think I can know this is a "normalize" traversal, or really any function which
				// requires knowledge of a closed over variable in a handler function.
			}
		}
	}

	normalize(operatorValue, operator, options, query/*, parent, parentKey*/) {
		this.validate(operatorValue, operator, query);
	}

	validate(operatorValue, operator, query) {
		if (!_.isPlainObject(operatorValue) || _.isEmpty(operatorValue)) {
			throw new QueryValidationError(`Argument to ${operator} must be a plain object`);
		}
		for (let relative in operatorValue) {
			if (relative[0] === '$') { continue; }
			try {
				require('../index').model(relative);
			} catch(err) {
				let msg = `Related model in ${operator} must be globally registered`;
				throw new QueryValidationError(msg, { query }, err);
			}
		}
	}

}
module.exports = exports = QueryOperatorRelated;
