const XError = require('xerror');

/**
 * Error class for errors from ES.  Constructor takes same arguments as XError.
 *
 * @class ElasticsearchError
 * @constructor
 */
class ElasticsearchError extends XError {

	constructor(...args) {
		super(...args);
	}

	/**
	 * Converts an error from an ElasticSearch response to an XError.
	 *
	 * @method fromESError
	 * @static
	 * @param {Number} httpStatusCode - The status code of the ES response
	 * @param {String|Object} responseBody - The (parsed or unparsed) ES response body.  Note that
	 *   not all errors may be parseable.
	 * @return {ElasticsearchError}
	 */
	static fromESError(httpStatusCode, responseBody) {

	}

}

// Register the XError code with default message
XError.registerErrorCode('db_error', {
	message: 'Internal database error',
	http: 500
});

module.exports = ElasticsearchError;
