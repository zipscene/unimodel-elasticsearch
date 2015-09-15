const chai = require('chai');
chai.use(require('chai-as-promised'));
const { expect } = chai;
const XError = require('xerror');

const { ElasticsearchError } = require('../lib');

describe.skip('ElasticsearchError', function() {

	it('should parse HTTP status codes into proper XError codes', function() {
		let err = ElasticsearchError.fromESError('409', 'DocumentAlreadyExistsException');
		expect(err.code).to.equal(XError.ALREADY_EXISTS);
		expect(err.data).to.equal('DocumentAlreadyExistsException');
	});

});
