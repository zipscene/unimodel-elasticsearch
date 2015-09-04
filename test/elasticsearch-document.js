const { expect } = require('chai');
const XError = require('xerror');

const testUtils = require('./lib/test-utils');
const { ElasticsearchDocument } = require('../lib');

describe('ElasticsearchDocument', function() {

	describe('#save', function() {

		let models, dog;
		before(function() {
			return testUtils.resetAndConnect()
				.then(() => {
					models = testUtils.createTestModels();
					dog = models.Animal.create({
						animalId: 'dog-charles-barkley-male',
						name: 'Charles Barkley',
						sex: 'male',
						description: 'a little asshole.'
					});
				});
		});

		it('should be able to save itself to the database', function() {
			return dog.save()
				.then(() => testUtils.getConnection().getClient())
				.then((client) => client.get({
					index: dog.getIndexId(),
					type: dog.getType(),
					id: dog.getInternalId()
				}))
				.then((esdata) => {
					let esdog = ElasticsearchDocument.fromESData(models.Animal, esdata);
					expect(dog.getData()).to.deep.equal(esdog.getData());
				});
		});

		it('should be able to resave itself with chagnes', function() {
			const dataDog = dog.getData();
			dataDog.name = 'Ein';
			dataDog.description = 'what the hell is a data dog?';
			return dog.save()
				.then(() => testUtils.getConnection().getClient())
				.then((client) => client.get({
					index: dog.getIndexId(),
					type: dog.getType(),
					id: dog.getInternalId()
				}))
				.then((esdata) => {
					let esdog = ElasticsearchDocument.fromESData(models.Animal, esdata);
					expect(dog.getData()).to.deep.equal(esdog.getData());
					expect(dog.getData().name).to.equal('Ein');
				});
		});

		it('should be able to remove itself from the database', function() {
			let getParams = {
				index: dog.getIndexId(),
				type: dog.getType(),
				id: dog.getInternalId()
			};
			return dog.remove()
				.then(() => testUtils.getConnection().getClient())
				.then((client) => client.get(getParams))
				.then((esdata) => {
					throw new Error(`Found ${esdata} instead of getting a NOT_FOUND error`);
				})
				.catch((err) => {
					expect(err.code).to.be.equal(XError.NOT_FOUND);
				});
		});

	});
});
