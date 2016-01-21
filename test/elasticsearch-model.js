const chai = require('chai');
chai.use(require('chai-as-promised'));
const { expect } = chai;
const moment = require('moment');
const XError = require('xerror');
const { createSchema } = require('zs-common-schema');
const { QueryValidationError, createQuery } = require('zs-common-query');

const testUtils = require('./lib/test-utils');
const {
	ElasticsearchModel,
	ElasticsearchIndex,
	ElasticsearchDocument,
	ElasticsearchError
} = require('../lib');

let idxItr = 0;
function makePerson(initialize = true, keys) {
	let Person = new ElasticsearchModel('Person', {
		personId: { type: String, index: true, id: true, key: true },
		name: { type: String, index: true, key: true },
		sex: { type: String, enum: [ 'male', 'female', 'unknown' ] }
	}, ('uetest_person_' + idxItr++), testUtils.getConnection(), { initialize, keys });
	return Person;
}

describe('ElasticsearchModel', function() {

	let models;
	before(function() {
		this.timeout(0);
		return testUtils.resetAndConnect()
			.then(() => {
				models = testUtils.createTestModels();

				let charles = models.Animal.create({
					animalId: 'dog-charles-barkley-male',
					name: 'Charles Barkley',
					isDog: true,
					sex: 'male',
					description: 'A little asshole.',
					loc: [ 64, 56 ]
				}, { routing: 'Charles' });

				let baloo = models.Animal.create({
					animalId: 'opes-farm-dog-baloo',
					name: 'Baloo',
					isDog: true,
					sex: 'male',
					description: 'What is a data dog, anyway?',
					loc: [ 75, 67 ]
				}, { routing: 'Baloo' });

				let ein = models.Animal.create({
					animalId: 'data-dog-ein',
					name: 'Ein',
					isDog: false,
					sex: 'female',
					description: 'A little asshole.',
					loc: [ 84, 39 ]
				}, { routing: 'Ein' });

				return Promise.all([
					charles.save({ consistency: 'quorum', refresh: true }),
					baloo.save({ consistency: 'quorum', refresh: true }),
					ein.save({ consistency: 'quorum', refresh: true })
				]);
			});
	});

	describe('#constructor', function() {

		it('should automatically asynchronously initialize', function() {
			let Person = makePerson();
			return Person._ensureIndex(Person.defaultIndex)
				.then(() => new Promise((resolve) => setTimeout(resolve, 10)))
				.then(() => {
					// Index mapping promises gets an entry when initializing
					expect(Person.indexMappingPromises).to.not.be.empty;
				});
		});

		it('should not automatically initialize if option is given', function() {
			let Person = makePerson(false);
			return Person._ensureIndex(Person.defaultIndex)
				.then(() => new Promise((resolve) => setTimeout(resolve, 10)))
				.then(() => {
					// Index mapping promises gets an entry when initializing
					expect(Person.indexMappingPromises).to.be.empty;
				});
		});

	});

	describe('#index', function() {

		it('should add extra indexes if the model hasn\'t been initialized', function() {
			let Person = makePerson();
			Person.index('name', { index: 'analyzed', analyzer: 'english', name: 'englishKeywords' });
			return Person.initialize().then(() => {
				expect(Person.extraIndexes).to.not.be.empty;
			});
		});

		it('should throw an error if trying to initialize after mapping has been set up', function() {
			let Person = makePerson();
			return Person.initialize().then(() => {
				expect(() => Person.index('name', { index: 'analyzed', analyzer: 'english', name: 'englishKeywords' }))
					.to.throw(XError, /Cannot add indexes to model after initialization has started/);
			});
		});

	});

	describe('#getKeys', function() {

		it('should support getting keys as constructor options', function() {
			let Person = makePerson(false, [ 'personId' ]);
			expect(Person.getKeys()).to.deep.equal([ 'personId' ]);
		});

		it('should support getting keys as schema properties', function() {
			let Person = makePerson(false, null);
			expect(Person.getKeys()).to.deep.equal([ 'personId', 'name' ]);
		});

		it('should support getting key as the first indexed schema property', function() {
			let Person = makePerson(false, null);
			Person.schema = createSchema({
				personId: { type: String, index: true },
				name: { type: String, index: true }
			});
			expect(Person.getKeys()).to.deep.equal([ 'personId' ]);
		});

		it('should throw an error if no method of getting keys is found', function() {
			let Person = makePerson(false, null);
			Person.schema = createSchema({ personId: String, name: String });
			expect(() => Person.getKeys())
				.to.throw(XError, /No keys are declared for this model/);
		});

	});

	describe('#initialize', function() {

		it('should ensure the index is created, mapping is put into the index, and resolve with the index', function() {
			let Person = makePerson(false);
			expect(Person.mapping).to.not.exist;
			expect(Person.indexMappingPromises).to.be.empty;
			return Person.initialize().then((index) => {
				expect(index).to.be.instanceof(ElasticsearchIndex);
				expect(Person.mapping).to.exist;
				expect(Person.indexMappingPromises).to.not.be.empty;
			});
		});

	});

	describe('#getMapping', function() {

		it('should wait until the model is initialized before given a mapping', function() {
			return models.Animal.getMapping().then((mapping) => {
				expect(models.Animal.indexMappingPromises).to.not.be.empty;
				expect(mapping).to.exist;
			});
		});

	});

	describe('#getName', function() {

		it('should return the internal typeName property', function() {
			expect(models.Animal.getName()).to.equal('Animal');
		});

	});

	describe('#create', function() {

		it('should create an ElasticsearchDocument cooresponding with this model', function() {
			let animal = models.Animal.create({
				animalId: 'dog-charles-barkley-male',
				name: 'Charles Barkley',
				sex: 'male',
				description: 'a little asshole.'
			});
			expect(animal.getModel()).to.equal(models.Animal);
			expect(animal.getType()).to.equal(models.Animal.getName());
		});

	});

	describe('#find', function() {

		it('should find all 3 documents', function() {
			return models.Animal.find({})
				.then((docs) => {
					expect(docs).to.be.instanceof(Array);
					expect(docs).to.have.length(3);
					for (let animal of docs) {
						expect(animal).to.be.instanceof(ElasticsearchDocument);
					}
				});
		});

		it('should find male documents', function() {
			return models.Animal.find({
				isDog: true
			}).then((docs) => {
				expect(docs).to.be.instanceof(Array);
				expect(docs).to.have.length(2);
				for (let animal of docs) {
					expect(animal).to.be.instanceof(ElasticsearchDocument);
					expect(animal.getData().isDog).to.be.true;
				}
			});
		});

		it('should find all documents close to a location and order them by distance', function() {
			let query = {
				loc: {
					$near: {
						$geometry: {
							type: 'Point',
							coordinates: [ 84, 39 ]
						},
						$maxDistance: 6731000
					}
				}
			};
			let commonQuery = createQuery(query);
			return models.Animal.find(query).then((docs) => {
				expect(docs).to.have.length(3);
				let previousDistance = 0;
				let currentDistance;
				for (let animal of docs) {
					commonQuery.matches(animal.getData());
					currentDistance = commonQuery.getMatchProperty('distance');
					expect(currentDistance).to.be.at.least(previousDistance);
				}
			});
		});

		it('should fail to normalize bad queries', function() {
			expect(() => models.Animal.find({ sex: { $what: { $what: '$what' } } }))
				.to.throw(QueryValidationError, 'Unrecognized expression operator: $what');
		});

		it('should fail to convert bad queries', function() {
			expect(() => models.Animal.find({ sex: 'male' }))
				.to.throw(QueryValidationError, 'Field is not indexed: sex');
		});

		describe('options', function() {

			it('skip', function() {
				return models.Animal.find({}, { skip: 1 })
					.then((docs) => {
						expect(docs).to.be.instanceof(Array);
						expect(docs).to.have.length(2);
						for (let animal of docs) {
							expect(animal).to.be.instanceof(ElasticsearchDocument);
						}
					});
			});

			it('limit', function() {
				return models.Animal.find({}, { limit: 1 })
					.then((docs) => {
						expect(docs).to.be.instanceof(Array);
						expect(docs).to.have.length(1);
						for (let animal of docs) {
							expect(animal).to.be.instanceof(ElasticsearchDocument);
						}
					});
			});

			it('fields', function() {
				return models.Animal.find({}, { fields: { isDog: 0 } })
					.then((docs) => {
						expect(docs).to.be.instanceof(Array);
						expect(docs).to.have.length(3);
						for (let animal of docs) {
							expect(animal.getData().animalId).to.exist;
							expect(animal.getData().isDog).to.not.exist;
						}
					});
			});

			it('total', function() {
				return models.Animal.find({}, { limit: 2, total: true })
					.then((docs) => {
						expect(docs).to.be.instanceof(Array);
						expect(docs).to.have.length(2);
						expect(docs.total).to.equal(3);
					});
			});

			it('sort', function() {
				return models.Animal.find({}, { sort: { name: 1, description: -1 } })
					.then((docs) => {
						expect(docs).to.be.instanceof(Array);
						expect(docs).to.have.length(3);
						let lastData = null;
						for (let animal of docs) {
							let data = animal.getData();
							if (lastData !== null) {
								expect(data.description).to.be.lte(lastData.description);
								if (data.description === lastData.description) {
									expect(data.name).to.be.gte(lastData.name);
								}
							}
							lastData = data;
						}
					});
			});

			it('index', function() {
				return models.Animal._ensureIndex('uetest_fakeanimals')
					.then((index) => models.Animal._ensureMapping(index))
					.then(() => {
						let fakeAnimal = models.Animal.create({
							animalId: 'data-dog-ein',
							name: 'Ein',
							sex: 'male',
							description: 'What is a data dog, anyway?'
						}, { index: 'uetest_fakeanimals' });
						return fakeAnimal.save({ consistency: 'quorum', refresh: true });
					})
					.then(() => models.Animal.find({}, { index: 'uetest_fakeanimals' }))
					.then((docs) => {
						expect(docs).to.be.instanceof(Array);
						expect(docs).to.have.length(1);
						for (let animal of docs) {
							expect(animal).to.be.instanceof(ElasticsearchDocument);
							expect(animal.getIndexId()).to.equal('uetest_fakeanimals');
						}
					});
			});

			it('routing', function() {
				return models.Animal.find({}, { routing: 'Ein' })
					.then((docs) => {
						expect(docs).to.be.instanceof(Array);
						expect(docs).to.not.have.length(3); // Means we hit a shart without the other dogs
					});
			});

		});
	});

	describe('#findStream', function() {

		it('should find male documents', function() {
			let docStream = models.Animal.findStream({ isDog: true });
			// Duck type check if this looks like a stream
			expect(docStream).to.be.an('object');
			expect(docStream.read).to.be.a('function');
			expect(docStream.on).to.be.a('function');
			return docStream.intoArray()
				.then((docs) => {
					expect(docs).to.be.instanceof(Array);
					expect(docs).to.have.length(2);
					for (let animal of docs) {
						expect(animal).to.be.instanceof(ElasticsearchDocument);
						expect(animal.getData().isDog).to.be.true;
					}
				});
		});

		it('should handle an query with no matches', function() {
			let docStream = models.Animal.findStream({ name: 'Larry Bird' });
			return docStream.intoArray()
				.then((docs) => {
					expect(docs).to.be.instanceof(Array);
					expect(docs).to.have.length(0);
				});
		});

		it('should have getTotal', function() {
			let docStream = models.Animal.findStream({ isDog: true });
			return docStream.intoArray()
				.then(() => docStream.getTotal())
				.then((total) => {
					expect(total).to.equal(2);
				});
		});

		it('should find with options', function() {
			return models.Animal._ensureIndex('uetest_fakeanimals_stream')
				.then((index) => models.Animal._ensureMapping(index))
				.then(() => {
					let fakeAnimalA = models.Animal.create(
						{ animalId: 'data-dog-a', isDog: true },
						{ routing: 'Ein', index: 'uetest_fakeanimals_stream' }
					);
					let fakeAnimalB = models.Animal.create(
						{ animalId: 'data-dog-b', isDog: true },
						{ routing: 'Ein', index: 'uetest_fakeanimals_stream' }
					);
					let fakeAnimalC = models.Animal.create(
						{ animalId: 'data-dog-c', isDog: true },
						{ routing: 'Ein', index: 'uetest_fakeanimals_stream' }
					);
					let fakeAnimalD = models.Animal.create(
						{ animalId: 'data-dog-d', isDog: true },
						{ routing: 'Ein', index: 'uetest_fakeanimals_stream' }
					);
					let fakeAnimalE = models.Animal.create(
						{ animalId: 'data-dog-e', isDog: false },
						{ routing: 'Ein', index: 'uetest_fakeanimals_stream' }
					);
					let fakeAnimalF = models.Animal.create(
						{ animalId: 'data-dog-f', isDog: true },
						{ routing: 'Baloo', index: 'uetest_fakeanimals_stream' }
					);
					return Promise.all([
						fakeAnimalA.save({ consistency: 'quorum', refresh: true }),
						fakeAnimalB.save({ consistency: 'quorum', refresh: true }),
						fakeAnimalC.save({ consistency: 'quorum', refresh: true }),
						fakeAnimalD.save({ consistency: 'quorum', refresh: true }),
						fakeAnimalE.save({ consistency: 'quorum', refresh: true }),
						fakeAnimalF.save({ consistency: 'quorum', refresh: true })
					]);
				})
				.then(() => models.Animal.findStream({ isDog: true }, {
					skip: 1,
					limit: 2,
					fields: { isDog: 0 },
					sort: { animalId: 1 },
					index: 'uetest_fakeanimals_stream',
					routing: 'Ein',
					scrollSize: 1,
					scrollTimeout: '2m'
				}))
				.then((docStream) => {
					return docStream.intoArray()
						.then((docs) => {
							expect(docs).to.be.instanceof(Array);
							expect(docs).to.have.length(2); // Test skip/limit
							let lastId = null;
							for (let animal of docs) {
								let data = animal.getData();
								expect(data.animalId).to.exist;
								expect(data.isDog).to.not.exist; // Test fields
								if (lastId !== null) {
									expect(lastId).to.be.lte(data.animalId); // Test sort
								}
								lastId = data.animalId;
							}
						})
						.then(() => docStream.getTotal())
						.then((total) => {
							expect(total).to.equal(4); // Test ElasticsearchDocumentStrem#getTotal
						});
				});
		});

		it('should fail to normalize bad queries', function() {
			expect(() => models.Animal.find({ sex: { $what: { $what: '$what' } } }))
				.to.throw(QueryValidationError, 'Unrecognized expression operator: $what');
		});

		it('should fail to convert bad queries', function() {
			expect(() => models.Animal.find({ sex: 'male' }))
				.to.throw(QueryValidationError, 'Field is not indexed: sex');
		});

	});

	describe('#update', function() {

		it('should update a stream of documents', function() {
			return models.Animal.update(
				{ isDog: true },
				{ $set: { updatable: 'whoa, dude!' } },
				{ consistency: 'quorum', refresh: true }
			)
				.then(() => models.Animal.find({ isDog: true }))
				.then((docs) => {
					expect(docs).to.be.instanceof(Array);
					expect(docs).to.have.length(2);
					for (let animal of docs) {
						let data = animal.getData();
						expect(data.isDog).to.be.true;
						expect(data.updatable).to.equal('whoa, dude!');
					}
				});
		});

		it('should fail to normalize a bad update', function() {
			expect(() => models.Animal.update({}, { $what: {} }))
				.to.throw(Error, 'operator: $what');
		});

	});

	describe('#count', function() {

		it('should count the number of documents given a query', function() {
			return models.Animal.count({ isDog: true })
				.then((count) => {
					expect(count).to.equal(2);
				});
		});

	});

	describe('#insert', function() {

		it('should insert one document', function() {
			return models.Animal.insert(
				{ animalId: 'charles', isDog: true, name: 'Charles' },
				{ index: 'uetest_fakeanimals_insert', consistency: 'quorum', refresh: true }
			)
				.then(() => models.Animal.find({}, { index: 'uetest_fakeanimals_insert' }))
				.then((docs) => {
					// Check retrun form find after they should be inserted
					expect(docs).to.be.instanceof(Array);
					expect(docs).to.have.length(1);
					for (let animal of docs) {
						expect(animal).to.be.instanceof(ElasticsearchDocument);
					}
				});
		});

	});

	describe('#insertMulti', function() {

		it('should insert multiple documents', function() {
			return models.Animal.insertMulti([
				{ animalId: 'charles', isDog: true, name: 'Charles' },
				{ animalId: 'baloo', isDog: true, name: 'Baloo' },
				{ animalId: 'ein', isDog: false, name: 'Ein' }
			], {
				index: 'uetest_fakeanimals_insertmulti',
				consistency: 'quorum',
				refresh: true
			})
				.then(() => models.Animal.find({}, { index: 'uetest_fakeanimals_insertmulti' }))
				.then((docs) => {
					// Check retrun form find after they should be inserted
					expect(docs).to.be.instanceof(Array);
					expect(docs).to.have.length(3);
					for (let animal of docs) {
						expect(animal).to.be.instanceof(ElasticsearchDocument);
					}
				});
		});

	});

	describe('#remove', function() {

		it('should remove documents', function() {
			return models.Animal.insertMulti([
				{ animalId: 'charles', isDog: true, name: 'Charles' },
				{ animalId: 'baloo', isDog: true, name: 'Baloo' },
				{ animalId: 'ein', isDog: false, name: 'Ein' }
			], {
				index: 'uetest_fakeanimals_remove',
				consistency: 'quorum',
				refresh: true
			})
				.then(() => models.Animal.remove({ isDog: true }, {
					index: 'uetest_fakeanimals_remove'
				}))
				.then(() => models.Animal.find({}, { index: 'uetest_fakeanimals_remove' }))
				.then((docs) => {
					expect(docs).to.be.instanceof(Array);
					expect(docs).to.have.length(1);
					for (let animal of docs) {
						expect(animal).to.be.instanceof(ElasticsearchDocument);
					}
				});
		});

	});

	describe('#aggregateMulti', function() {

		before(() => models.Animal.insertMulti([
			{
				animalId: 'charles-1',
				name: 'Charles',
				found: moment.utc('2015-01-01', 'YYYY-MM-DD').toDate(),
				age: 0
			},
			{
				animalId: 'charles-2',
				name: 'Charles',
				found: moment.utc('2015-01-02', 'YYYY-MM-DD').toDate(),
				age: 1
			},
			{
				animalId: 'baloo',
				name: 'Baloo',
				found: moment.utc('2015-01-11', 'YYYY-MM-DD').toDate(),
				age: 2
			},
			{
				animalId: 'ein-1',
				name: 'Ein',
				found: moment.utc('2015-02-01', 'YYYY-MM-DD').toDate(),
				age: 3
			},
			{
				animalId: 'ein-2',
				name: 'Ein',
				found: moment.utc('2015-02-11', 'YYYY-MM-DD').toDate(),
				age: 8
			}
		], {
			index: 'uetest_aggregates',
			consistency: 'quorum',
			refresh: true
		}));

		describe('single aggregate', function() {

			it('stats', function() {
				return models.Animal.aggregate({}, {
					stats: 'age'
				}, { index: 'uetest_aggregates' }).then((aggr) => {
					expect(aggr).to.deep.equal({
						stats: {
							age: {
								count: 5
							}
						}
					});
				});
			});

			it('total', function() {
				return models.Animal.aggregate({}, {
					total: true
				}, { index: 'uetest_aggregates' }).then((aggr) => {
					expect(aggr).to.deep.equal({
						total: 5
					});
				});
			});

			it('groupBy field', function() {
				return models.Animal.aggregate({}, {
					groupBy: 'name',
					stats: 'age',
					total: true
				}, { index: 'uetest_aggregates' }).then((aggr) => {
					expect(aggr).to.be.instanceof(Array);
					expect(aggr).to.have.length(3);
					let expected = [
						{
							keys: [ 'charles' ],
							stats: { age: { count: 2 } },
							total: 2
						},
						{
							keys: [ 'baloo' ],
							stats: { age: { count: 1 } },
							total: 1
						},
						{
							keys: [ 'ein' ],
							stats: { age: { count: 2 } },
							total: 2
						}
					];
					expect(aggr).to.deep.have.members(expected);
				});
			});

			it('groupBy date range', function() {
				let dateJan = moment('2015-01-02', 'YYYY-MM-DD').toDate();
				let dateFeb = moment('2015-02-01', 'YYYY-MM-DD').toDate();
				return models.Animal.aggregate({}, {
					groupBy: { // Date Range
						field: 'found',
						ranges: [
							{ end: dateJan },
							{ start: dateJan, end: dateFeb },
							{ start: dateFeb }
						]
					},
					total: true
				}, { index: 'uetest_aggregates' }).then((aggr) => {
					expect(aggr).to.be.instanceof(Array);
					expect(aggr).to.have.length(3);
					expect(aggr).to.deep.include.members([
						{
							keys: [ 0 ],
							total: 1
						},
						{
							keys: [ 1 ],
							total: 2
						},
						{
							keys: [ 2 ],
							total: 2
						}
					]);
				});

			});

			it('groupBy numeric range', function() {
				return models.Animal.aggregate({}, {
					groupBy: { // Numeric Range
						field: 'age',
						ranges: [
							{ end: 2 },
							{ start: 2, end: 5 },
							{ start: 5 }
						]
					},
					total: true
				}, { index: 'uetest_aggregates' }).then((aggr) => {
					expect(aggr).to.be.instanceof(Array);
					expect(aggr).to.have.length(3);
					expect(aggr).to.deep.include.members([
						{
							keys: [ 0 ],
							total: 2
						},
						{
							keys: [ 1 ],
							total: 2
						},
						{
							keys: [ 2 ],
							total: 1
						}
					]);
				});
			});

			it('groupBy date interval', function() {
				return models.Animal.aggregate({}, {
					groupBy: { // Date Interval
						field: 'found',
						interval: 'P1M'
					},
					total: true
				}, { index: 'uetest_aggregates' }).then((aggr) => {
					expect(aggr).to.be.instanceof(Array);
					expect(aggr).to.have.length(3);
					expect(aggr).to.deep.include.members([
						{
							keys: [ '2014-12-06T00:00:00.000Z' ],
							total: 2
						},
						{
							keys: [ '2015-01-05T00:00:00.000Z' ],
							total: 2
						},
						{
							keys: [ '2015-02-04T00:00:00.000Z' ],
							total: 1
						}
					]);
				});
			});

			it('groupBy numeric interval', function() {
				return models.Animal.aggregate({}, {
					groupBy: { // Numeric Interval
						field: 'age',
						interval: 2
					},
					total: true
				}, { index: 'uetest_aggregates' }).then((aggr) => {
					expect(aggr).to.be.instanceof(Array);
					expect(aggr).to.have.length(3);
					expect(aggr).to.deep.include.members([
						{
							keys: [ 0 ],
							total: 2
						},
						{
							keys: [ 2 ],
							total: 2
						},
						{
							keys: [ 8 ],
							total: 1
						}
					]);
				});
			});

			it('groupBy time component', function() {
				return models.Animal.aggregate({}, {
					groupBy: { // Time Component
						field: 'found',
						timeComponent: 'day',
						timeComponentCount: 2
					},
					total: true
				}, { index: 'uetest_aggregates' }).then((aggr) => {
					expect(aggr).to.be.instanceof(Array);
					expect(aggr).to.have.length(4);
					expect(aggr).to.deep.include.members([
						{
							keys: [ '2015-01-01T00:00:00.000Z' ],
							total: 2
						},
						{
							keys: [ '2015-01-11T00:00:00.000Z' ],
							total: 1
						},
						{
							keys: [ '2015-01-31T00:00:00.000Z' ],
							total: 1
						},
						{
							keys: [ '2015-02-10T00:00:00.000Z' ],
							total: 1
						}
					]);
				});
			});

			it('complex', function() {
				let dateJan = moment.utc('2015-01-01', 'YYYY-MM-DD').toDate();
				let dateFeb = moment.utc('2015-02-01', 'YYYY-MM-DD').toDate();
				let aggr = {
					stats: 'age',
					groupBy: [
						{ // Terms
							field: 'name'
						},
						{ // Date Range
							field: 'found',
							ranges: [
								{ end: dateJan },
								{ start: dateJan, end: dateFeb },
								{ start: dateFeb }
							]
						},
						{ // Numeric Range
							field: 'age',
							ranges: [
								{ end: 2 },
								{ start: 2, end: 5 },
								{ start: 5 }
							]
						},
						{ // Date Interval
							field: 'found',
							interval: 'P1M'
						},
						{ // Numeric Interval
							field: 'age',
							interval: 2
						},
						{ // Time Component
							field: 'found',
							timeComponent: 'day',
							timeComponentCount: 2
						}
					],
					total: true
				};
				return models.Animal.aggregate({}, aggr, { index: 'uetest_aggregates' }).then((aggr) => {
					expect(aggr).to.be.instanceof(Array);
					expect(aggr).to.have.length(4);
					expect(aggr).to.deep.include.members([
						{
							keys: [ 'charles', 1, 0, '2014-12-06T00:00:00.000Z', 0, '2015-01-01T00:00:00.000Z' ],
							stats: {
								age: { count: 2 }
							},
							total: 2
						},
						{
							keys: [ 'ein', 2, 1, '2015-01-05T00:00:00.000Z', 2, '2015-01-31T00:00:00.000Z' ],
							stats: {
								age: { count: 1 }
							},
							total: 1
						},
						{
							keys: [ 'ein', 2, 2, '2015-02-04T00:00:00.000Z', 8, '2015-02-10T00:00:00.000Z' ],
							stats: {
								age: { count: 1 }
							},
							total: 1
						},
						{
							keys: [ 'baloo', 1, 1, '2015-01-05T00:00:00.000Z', 2, '2015-01-11T00:00:00.000Z' ],
							stats: {
								age: { count: 1 }
							},
							total: 1
						}
					]);
				});
			});

		});

		describe('multiple aggregates', function() {

			it('complex', function() {
				return models.Animal.aggregateMulti({}, {
					stats: {
						stats: 'age'
					},
					field: {
						groupBy: 'name',
						total: true
					},
					multi: {
						groupBy: [
							'name',
							{
								field: 'age',
								ranges: [
									{ end: 2 },
									{ start: 2, end: 5 },
									{ start: 5 }
								]
							}
						],
						total: true
					}
				}, { index: 'uetest_aggregates' }).then((aggrs) => {
					expect(aggrs.stats).to.deep.equal({
						stats: {
							age: {
								count: 5
							}
						}
					});
					expect(aggrs.field).to.deep.include.members([
						{
							keys: [ 'charles' ],
							total: 2
						},
						{
							keys: [ 'baloo' ],
							total: 1
						},
						{
							keys: [ 'ein' ],
							total: 2
						}
					]);
					expect(aggrs.multi).to.deep.include.members([
						{
							keys: [ 'charles', 0 ],
							total: 2
						},
						{
							keys: [ 'baloo', 1 ],
							total: 1
						},
						{
							keys: [ 'ein', 1 ],
							total: 1
						},
						{
							keys: [ 'ein', 2 ],
							total: 1
						}
					]);
				});
			});

		});

	});

});
