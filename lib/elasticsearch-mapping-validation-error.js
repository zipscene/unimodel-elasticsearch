const XError = require('xerror');

XError.registerErrorCode('elasticsearchMapping_validation_error', {
	message: 'Invalid ElasticSearch mapping',
	http: 400
});

/**
 * Class representing an error validating an elasticsearch mapping.
 *
 * @class ElasticsearchMappingValidationError
 * @extends XError
 * @constructor
 * @param {String} reason - String reason for the validation error
 * @param {Object} [data] - Additional data about the validation error
 * @param {Error} [cause] - The error instance that triggered this error
 */
class ElasticsearchMappingValidationError extends XError {

	constructor(reason, data, cause) {
		super(XError.ELASTICSEARCHMAPPING_VALIDATION_ERROR, 'Invalid ElasticSearch mapping: ' + reason, data, cause);
	}

}

module.exports = ElasticsearchMappingValidationError;
