const _ = require('lodash');
const objtools = require('zs-objtools');
const {
	QueryOperator,
	QueryValidationError
} = require('zs-common-query');
const XError = require('xerror');

/**
 * The $parent operator, which is only implemented in ElasticSearch
 */
class QueryOperatorRelated extends QueryOperator {

	constructor(name) {
		super(name || '$related');
	}

	matches(/*value, operatorValue, operator, options, query*/) {
		// We can't support matches operations on child/parent queries
		throw new XError(XError.UNSUPPORTED_OPERATION);
	}

	traverse(/*operatorValue, operator, query, handlers*/) {
		// "Relative" operators should be treated as leafs
		return;
	}

	/**
	 * Helper function to facilitate common operations between validate and normalize.
	 * If this is a normalize operation, the data is replaced, otherwise, it is just validated.
	 *
	 * @method _normalizeOrValidate
	 * @private
	 * @param {Mixed} operatorValue
	 * @param {String} operator
	 * @param {Query} query
	 * @param {Object} [options={}]
	 * @param {Boolean} [doNormalize=false] - Wether or not to perform a normalize operation
	 *   (otherwise this will do a validate)
	 */
	_normalizeOrValidate(operatorValue, operator, query, options = {}, doNormalize = false) {
		if (!_.isPlainObject(operatorValue) || _.isEmpty(operatorValue)) {
			throw new QueryValidationError(`Argument to ${operator} must be a plain object`);
		}
		for (let relative in operatorValue) {
			if (relative[0] === '$') {
				// Handle relative operator options (currently only accepts numbers as [$maxChildren|$minChildren])
				if (operator !== '$child' || (relative !== '$maxChildren' && relative !== '$minChildren')) {
					let msg = `Unexpected query operator ${operator} value: ${relative}`;
					throw new QueryValidationError(msg, { query });
				}
				if (doNormalize) { // Normalize into a number
					operatorValue[relative] = parseInt(operatorValue[relative]);
				}
				if (!_.isNumber(operatorValue[relative]) || isNaN(operatorValue[relative])) {
					let msg = `Query operator option ${relative} must be a number`;
					throw new QueryValidationError(msg, { query });
				}
			} else {
				// Otherwise, validate the subquery
				let Relative;
				try { // Try to get a registered Related model
					Relative = require('../index').model(relative);
				} catch(err) {
					let msg = `Related model in ${operator} must be globally registered`;
					throw new QueryValidationError(msg, { query }, err);
				}
				const { createQuery } = require('../common-query'); // Try to prevent circlular dependencies
				let relativeOptions = objtools.deepCopy(options);
				if (doNormalize) {
					if (relativeOptions.schema) {
						// If schema is getting passed around, also include the schema with this
						relativeOptions.schema = Relative.getSchema();
					}
					let relativeQuery = createQuery(operatorValue[relative], relativeOptions);
					operatorValue[relative] = relativeQuery.getData();
				} else {
					relativeOptions.validate = false;
					let relativeQuery = createQuery(operatorValue[relative], relativeOptions);
					relativeQuery.validate();
				}
			}
		}
		if (operatorValue.$minChildren && operatorValue.$maxChildren) {
			let msg = 'value of $minChildren must not be greater than value of $maxChildren';
			throw new QueryValidationError(msg, { query });
		}
	}

	normalize(operatorValue, operator, options, query/*, parent, parentKey*/) {
		this._normalizeOrValidate(operatorValue, operator, query, options, true);
	}

	validate(operatorValue, operator, query) {
		this._normalizeOrValidate(operatorValue, operator, query);
	}

}
module.exports = exports = QueryOperatorRelated;
