const chai = require('chai');
chai.use(require('chai-as-promised'));
const { expect } = chai;
const XError = require('xerror');

const testUtils = require('./lib/test-utils');
const { ElasticsearchIndexManager } = require('../lib');

function makeIndexManager() {
	return new ElasticsearchIndexManager(testUtils.getConnection(), testUtils.getConfig().indexConfigs);
}

describe.skip('ElasticsearchIndexManager', function() {

	before(testUtils.resetAndConnect);

	describe('#getIndex', function() {

		it('should create an index if a valid exact config exists', function() {
			let indexManager = makeIndexManager();
			return indexManager.getIndex('uitest_what')
				.then((index) => {
					expect(index.getName()).to.equal('uitest_what');
				});
		});

		it('should create an index if a valid glob config exists', function() {
			let indexManager = makeIndexManager();
			return indexManager.getIndex('uetest_dog')
				.then((index) => {
					expect(index.getName()).to.equal('uetest_dog');
				});
		});

		it('should throw an error if no valid config exists', function() {
			let indexManager = makeIndexManager();
			expect(indexManager.getIndex('nothing_index'))
				.to.be.rejectedWith(XError, /No matching ES index config found for: nothing_index/);
		});

		it('should not create a new index if an index has already been created', function() {
			let indexManager = makeIndexManager();
			return indexManager.getIndex('uetest_exists')
				.then((index) => {
					return indexManager.getIndex('uetest_exists')
						.then((otherIndex) => {
							expect(index).to.equal(otherIndex);
						});
				});
		});

	});

});
