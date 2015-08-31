const expect = require('chai').expect;
const testUtils = require('./lib/test-utils');

describe.skip('Example', function() {

	beforeEach(testUtils.resetAndConnect);

	it('should fail because this is just an example test', function(done) {
		expect(false).to.equal(true);
		done();
	});

});


