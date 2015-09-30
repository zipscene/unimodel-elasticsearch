const chai = require('chai');
chai.use(require('chai-as-promised'));
const { expect } = chai;
const XError = require('xerror');

const testUtils = require('./lib/test-utils');
const { ElasticsearchDocument } = require('../lib');

describe('ElasticsearchDocument', function() {


	let models, dog;
	before(function() {
		this.timeout(0);
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

	it('should be able to save itself to the database and check itself using #fromESData', function() {
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
		let removeGetPromise = dog.remove()
			.then(() => testUtils.getConnection().getClient())
			.then((client) => client.get(getParams));
		return expect(removeGetPromise).to.be.rejectedWith(XError, /Not Found/);
	});

	it('should create objects with parent-child relationships', function() {
		let shelter = models.Shelter.create({
			shelterId: 'opes-farm',
			name: 'Ope\'s Farm'
		});
		let baloo = models.ShelteredAnimal.create({
			animalId: 'opes-farm-dog-baloo',
			name: 'Baloo',
			sex: 'male',
			description: 'A big slobery dog',
			found: new Date('2012-09-04T17:00:00.000Z')
		});
		return shelter.save()
			.then(() => {
				baloo.setParentId(shelter.getInternalId());
				return baloo.save();
			})
			.then(() => testUtils.getConnection().getClient())
			.then((client) => client.get({
				index: baloo.getIndexId(),
				type: baloo.getType(),
				id: baloo.getInternalId(),
				parent: baloo.getParentId(),
				fields: [ '_routing', '_parent', '_source', '_id', '_index' ]
			}))
			.then((esdata) => {
				let esdog = ElasticsearchDocument.fromESData(models.Animal, esdata);
				expect(baloo.getData()).to.deep.equal(esdog.getData());
				expect(baloo.getParentId()).to.equal(esdog.getParentId());
			});
	});

	it('should fail to create child objects that require parents', function() {
		let shelterlessDog = models.ShelteredAnimal.create({
			animalId: 'shelterless-dog'
		});
		return expect(shelterlessDog.save())
			.to.be.rejectedWith(XError, /Parent ID is required for child models/);
	});

	it('should fail to delete child objects without originalData', function() {
		let dog = models.Animal.create({
			animalId: 'dog-charles-barkley-male',
			name: 'Charles Barkley',
			sex: 'male',
			description: 'a little asshole.'
		});
		return expect(dog.remove())
			.to.be.rejectedWith(XError, /Cannot remove document that did not originally exist/);
	});

	it('should fail to delete if no internalID/index field is provided', function() {
		let dog = models.Animal.create({
			animalId: 'dog-charles-barkley-male',
			name: 'Charles Barkley',
			sex: 'male',
			description: 'a little asshole.'
		});
		return dog.save()
			.then(() => {
				dog._originalFields.id = null;
				return expect(dog.remove())
					.to.be.rejectedWith(XError, /Cannot remove document without internal ID or index fields/);
			});
	});

	it('should fail to delete if no parent ID is provided', function() {
		let shelter = models.Shelter.create({
			shelterId: 'opes-farm',
			name: 'Ope\'s Farm'
		});
		let baloo = models.ShelteredAnimal.create({
			animalId: 'opes-farm-dog-baloo',
			name: 'Baloo',
			sex: 'male',
			description: 'A big slobery dog',
			found: new Date('2012-09-04T17:00:00.000Z')
		});
		return shelter.save()
			.then(() => {
				baloo.setParentId(shelter.getInternalId());
				return baloo.save();
			})
			.then(() => {
				baloo.setParentId(null);
				return expect(baloo.remove())
					.to.be.rejectedWith(XError, /Parent ID is required for removing child models./);
			});
	});

	it('should fail to save with invalid "consistency" argument', function() {
		let invalid = models.Animal.create({ animalId: 'invalid' });
		expect(() => invalid.save({ consistency: 'none' }))
			.to.throw(XError.INVALID_ARGUMENT, 'Save consistency options must be one of: "one", "quorum", "all"');
	});

	it('should fail to save with invalid "replication" argument', function() {
		let invalid = models.Animal.create({ animalId: 'invalid' });
		expect(() => invalid.save({ replication: 'all-the-things' }))
			.to.throw(XError.INVALID_ARGUMENT, 'Replication types must be one of: "sync", "async"');
	});

});
