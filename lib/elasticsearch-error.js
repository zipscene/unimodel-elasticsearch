// Copyright 2016 Zipscene, LLC
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

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
	 * @param {Number} status - The status code of the ES response.
	 * @param {String|Object} message - The ElasticSearch error's message.
	 * @return {ElasticsearchError}
	 */
	static fromESError(status, message) {
		let statusCode = parseInt(status);
		for (let code of XError.listErrorCodes()) {
			let errorCode = XError.getErrorCode(code);
			if (errorCode.http === statusCode) {
				return new ElasticsearchError(code, errorCode.message, message);
			}
		}
		return new ElasticsearchError(
			XError.INTERNAL_ERROR,
			`Unknown http status: ${status}`,
			message
		);
	}

}

// Register the XError code with default message
XError.registerErrorCode('db_error', {
	message: 'Internal database error',
	http: 500
});

module.exports = ElasticsearchError;
