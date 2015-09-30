const { expect } = require('chai');

const testUtils = require('./lib/test-utils');

describe('ElasticsearchConnection', function() {

	let conn;
	before(function() {
		this.timeout(0);
		return testUtils.resetAndConnect()
			.then((conn_) => {
				conn = conn_;
			});
	});

	describe('request', function() {

		it('should resolve with a response body', function() {
			return conn.request({
				method: 'GET',
				path: '/'
			}).then((body) => {
				body = JSON.parse(body);
				expect(body.status).to.equal(200);
			});
		});

	});

	describe('requestStream', function() {

		it('should resolve with a response stream', function() {
			this.timeout(1000000);
			return conn.requestStream({
				method: 'GET',
				path: '/'
			}).then((stream) => stream.intoString())
				.then((body) => {
					body = JSON.parse(body);
					expect(body.status).to.equal(200);
				});
		});

	});

});
