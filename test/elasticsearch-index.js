const chai = require('chai');
chai.use(require('chai-as-promised'));
const { expect } = chai;
const { createSchema } = require('zs-common-schema');

const testUtils = require('./lib/test-utils');
const { ElasticsearchIndex } = require('../lib');
const { convertSchema } = require('../lib/convert');

let idxItr = 0;
function makeIndex(config = {}, name = ('uetest_idx_' + idxItr++)) {
	let index = new ElasticsearchIndex(name, testUtils.getConnection(), config);
	return index;
}

const EX_WARMER = {
	types: [],
	source: {
		query: {
			'match_all': {}
		},
		aggs: {
			one: {
				terms: {
					field: 'name'
				}
			}
		}
	}
};

const EX_ALIAS = {
	filter: {
		term: {
			year: 2014
		}
	}
};

describe('ElasticsearchIndex', function() {

	let dogIndex;
	before(() => testUtils.resetAndConnect().then(() => {
		dogIndex = new ElasticsearchIndex('uetest_dog', testUtils.getConnection(), testUtils.getConfig());
	}));

	describe('#initialize', function() {

		it('should initialize a new index', function() {
			let index = makeIndex();
			return index.indexWaiter.promise
				.then(() => testUtils.getConnection().getClient())
				.then((client) => client.indices.exists({ index: index.getName() }))
				.then((exists) => expect(exists).to.be.true);
		});

		it('should initialize a new index with warmers', function() {
			let index = makeIndex({
				warmers: {
					'getting_hot': EX_WARMER
				}
			});
			return index.indexWaiter.promise
				.then(() => testUtils.getConnection().getClient())
				.then((client) => client.indices.getWarmer({ index: index.getName(), name: 'getting_hot' }))
				.then((response) => {
					let warmer = response[index.getName()].warmers.getting_hot;
					expect(warmer).to.deep.equal(EX_WARMER);
				});
		});

		it('should initialize a new index with aliases', function() {
			let index = makeIndex({
				aliases: {
					'a_name': EX_ALIAS
				}
			});
			return index.indexWaiter.promise
				.then(() => testUtils.getConnection().getClient())
				.then((client) => client.indices.getAlias({ index: index.getName(), name: 'a_name' }))
				.then((response) => {
					let alias = response[index.getName()].aliases.a_name;
					expect(alias).to.deep.equal(EX_ALIAS);
				});
		});

		it('should initialize an existing index', function() {
			let index = makeIndex();
			return index.indexWaiter.promise.then(() => {
				let existingIndex = makeIndex({ replicas: 2 }, index.getName());
				return existingIndex.indexWaiter.promise;
			})
				.then(() => testUtils.getConnection().getClient())
				.then((client) => client.indices.getSettings({ index: index.getName() }))
				.then((response) => {
					let settings = response[index.getName()].settings.index;
					expect(parseInt(settings.number_of_replicas)).to.equal(2);
				});
		});

		it('should initialize an existing index with warmers', function() {
			this.timeout(3000);
			let index = makeIndex();
			return new Promise((resolve) => setTimeout(resolve, 1000))
				.then(() => index.indexWaiter.promise)
				.then(() => {
					let existingIndex = makeIndex({
						warmers: {
							'getting_hot': EX_WARMER
						}
					}, index.getName());
					return existingIndex.indexWaiter.promise;
				})
				.then(() => testUtils.getConnection().getClient())
				.then((client) => client.indices.getWarmer({ index: index.getName(), name: 'getting_hot' }))
				.then((response) => {
					let warmer = response[index.getName()].warmers.getting_hot;
					expect(warmer).to.deep.equal(EX_WARMER);
				});
		});

		it('should initialize an existing index with aliases', function() {
			let index = makeIndex();
			return index.indexWaiter.promise
				.then(() => {
					let existingIndex = makeIndex({
						aliases: {
							'a_name': EX_ALIAS
						}
					}, index.getName());
					return existingIndex.indexWaiter.promise;
				})
				.then(() => testUtils.getConnection().getClient())
				.then((client) => client.indices.getAlias({ index: index.getName(), name: 'a_name' }))
				.then((response) => {
					let alias = response[index.getName()].aliases.a_name;
					expect(alias).to.deep.equal(EX_ALIAS);
				});
		});

		it('should only initialize once', function() {
			let index = makeIndex();
			let connectedCount = 0;
			index.on('connected', () => connectedCount++);
			let ret = index.indexWaiter.promise
				.then(() => new Promise((resolve) => setTimeout(resolve, 2)))
				.then(() => index.initialize())
				.then(() => new Promise((resolve) => setTimeout(resolve, 2)))
				.then(() => {
					expect(connectedCount).to.equal(1);
				});
			index.initialize();
			return ret;
		});

	});

	describe('#getName', function() {

		it('should get the name of the index', function() {
			expect(dogIndex.getName()).to.equal('uetest_dog');
		});

	});

	describe('#addMapping', function() {

		it('should wait until the index is initialized, then add mapping', function() {
			let schema = createSchema({ dogId: { type: String, index: true }, name: String });
			let mapping = convertSchema(schema);
			return dogIndex.addMapping('Dog', mapping).then(() => {
				return testUtils.getConnection().getClient();
			})
				.then((client) => client.indices.getMapping({
					index: dogIndex.getName(),
					type: 'Dog'
				}))
				.then((mapping) => {
					expect(mapping[dogIndex.getName()].mappings.Dog).to.exist;
				});
		});

	});

});
