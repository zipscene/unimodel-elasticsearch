// Copyright 2016 Zipscene, LLC
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

const _ = require('lodash');
const { QueryValidationError } = require('common-query');
const QueryOperatorParent = require('./parent');

class QueryOperatorChild extends QueryOperatorParent {

	constructor(name, allowedOptions = [ '$minChildren', '$maxChildren' ]) {
		super(name, allowedOptions);
	}

	_normalizeOrValidate(operatorValue, operator, query, options = {}, doNormalize = false) {
		super._normalizeOrValidate(operatorValue, operator, query, options, doNormalize);

		// Normalize [$minChildren|$maxChildren]
		for (let childOption of [ '$minChildren', '$maxChildren' ]) {
			if (operatorValue[childOption] !== null && operatorValue[childOption] !== undefined) {
				if (doNormalize) {
					operatorValue[childOption] = parseInt(operatorValue[childOption]);
				}
				if (!_.isNumber(operatorValue[childOption]) || isNaN(operatorValue[childOption])) {
					let msg = `Query operator option ${childOption} must be a number`;
					throw new QueryValidationError(msg, { query });
				}
			}
		}
		// Ensure child options are within bounds
		if (
			_.isNumber(operatorValue.$minChildren) &&
			_.isNumber(operatorValue.$maxChildren) &&
			operatorValue.$minChildren > operatorValue.$maxChildren
		) {
			let msg = 'value of $minChildren must not be greater than value of $maxChildren';
			throw new QueryValidationError(msg, { query });
		}
	}

}
module.exports = exports = QueryOperatorChild;
