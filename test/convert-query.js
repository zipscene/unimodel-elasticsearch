const { expect } = require('chai');
const { QueryValidationError } = require('zs-common-query');

const { createQuery } = require('../lib/common-query');
const { convertQuery } = require('../lib/convert/query');
const testUtils = require('./lib/test-utils');

describe('convertQuery', function() {

	let models;
	before(() => testUtils.resetAndConnect()
		.then(() => {
			models = testUtils.createTestModels();
		}));

	describe('query expressions', function() {

		it('should convert an empty query', function() {
			let query = createQuery({});
			expect(convertQuery(query, models.Animal)).to.deep.equal({ 'match_all': {} });
		});

		it('should convert an exact match', function() {
			let query = createQuery({ animalId: 'charles-barkley-dog-male' });
			expect(convertQuery(query, models.Animal))
				.to.deep.equal({ term: { animalId: 'charles-barkley-dog-male' } });
		});

		it('$and', function() {
			let query = createQuery({
				$and: [
					{ animalId: 'charles-barkley-dog-male' },
					{ animalId: 'baloo-dog-male' }
				]
			});
			expect(convertQuery(query, models.Animal)).to.deep.equal({
				bool: { must: [
					{ term: { animalId: 'charles-barkley-dog-male' } },
					{ term: { animalId: 'baloo-dog-male' } }
				] }
			});
		});

		it('$nor', function() {
			let query = createQuery({
				$nor: [
					{ animalId: 'charles-barkley-dog-male' },
					{ animalId: 'baloo-dog-male' }
				]
			});
			expect(convertQuery(query, models.Animal)).to.deep.equal({
				bool: { 'must_not': [
					{ term: { animalId: 'charles-barkley-dog-male' } },
					{ term: { animalId: 'baloo-dog-male' } }
				] }
			});
		});

		it('$or single', function() {
			let query = createQuery({
				$or: [
					{ animalId: 'charles-barkley-dog-male' },
					{ animalId: 'baloo-dog-male' }
				]
			});
			expect(convertQuery(query, models.Animal)).to.deep.equal({
				bool: { should: [
					{ term: { animalId: 'charles-barkley-dog-male' } },
					{ term: { animalId: 'baloo-dog-male' } }
				] }
			});
		});

		it('$or multiple', function() {
			let query = createQuery({
				$and: [
					{ $or: [
						{ animalId: 'charles-barkley-dog-male' }
					] },
					{ $or: [
						{ animalId: 'baloo-dog-male' }
					] }
				]
			});
			expect(convertQuery(query, models.Animal)).to.deep.equal({
				bool: { must: [
					{ bool: { should: [
						{ term: { animalId: 'charles-barkley-dog-male' } }
					] } },
					{ bool: { should: [
						{ term: { animalId: 'baloo-dog-male' } }
					] } }
				] }
			});
		});

		it('$child', function() {
			let query = createQuery({
				$child: {
					ShelteredAnimal: {
						animalId: 'charles-barkley-dog-male'
					}
				}
			});
			expect(convertQuery(query, models.Shelter)).to.deep.equal({
				'has_child': {
					type: 'ShelteredAnimal',
					filter: { term: {
						animalId: 'charles-barkley-dog-male'
					} }
				}
			});
		});

		it('$parent', function() {
			let query = createQuery({
				$parent: {
					Shelter: {
						shelterId: 'opes-farm'
					}
				}
			});
			expect(convertQuery(query, models.ShelteredAnimal)).to.deep.equal({
				'has_parent': {
					type: 'Shelter',
					filter: { term: {
						shelterId: 'opes-farm'
					} }
				}
			});
		});

		it('$child with minChildren', function() {
			let query = createQuery({
				$child: {
					$minChildren: 5,
					ShelteredAnimal: {
						animalId: 'charles-barkley-dog-male'
					}
				}
			});
			expect(convertQuery(query, models.Shelter)).to.deep.equal({
				'has_child': {
					type: 'ShelteredAnimal',
					min_children: 5, //eslint-disable-line camelcase
					filter: { term: {
						animalId: 'charles-barkley-dog-male'
					} }
				}
			});
		});

		it('$child with maxChildren', function() {
			let query = createQuery({
				$child: {
					$maxChildren: 6,
					ShelteredAnimal: {
						animalId: 'charles-barkley-dog-male'
					}
				}
			});
			expect(convertQuery(query, models.Shelter)).to.deep.equal({
				'has_child': {
					type: 'ShelteredAnimal',
					max_children: 6, //eslint-disable-line camelcase
					filter: { term: {
						animalId: 'charles-barkley-dog-male'
					} }
				}
			});
		});

		it('should bubble $child/$parent errors', function() {
			let query = createQuery({
				$child: {
					ShelteredAnimal: {
						bedId: 'this-is-not-the-bed-youre-looking-for'
					}
				}
			});
			expect(() => convertQuery(query, models.Shelter))
				.to.throw(QueryValidationError, 'Could not find field bedId in schema');
		});

	});

	describe('operators expressions', function() {

		it('$text', function() {
			let query = createQuery({
				description: {
					$text: 'dog'
				}
			});
			expect(convertQuery(query, models.Animal)).to.deep.equal({
				query: { match: {
					description: {
						query: 'dog',
						operator: 'and'
					}
				} }
			});
		});

		it('$wildcard', function() {
			let query = createQuery({
				description: {
					$wildcard: 'dog*'
				}
			});
			expect(convertQuery(query, models.Animal)).to.deep.equal({
				regexp: {
					description: '^dog.*$'
				}
			});
		});

		it('$not', function() {
			let query = createQuery({
				description: { $not: { $wildcard: 'dog' } }
			});
			expect(convertQuery(query, models.Animal)).to.deep.equal({
				bool: { 'must_not': {
					regexp: { description: '^dog$' }
				} }
			});
		});

		it('$not mutli', function() {
			let query = createQuery({
				description: { $not: {
					$wildcard: 'dog',
					$text: 'dog'
				} }
			});
			expect(convertQuery(query, models.Animal)).to.deep.equal({
				bool: { 'must_not': [
					{ regexp: { description: '^dog$' } },
					{ query: { match: { description: { query: 'dog', operator: 'and' } } } }
				] }
			});
		});

		it('$exists: true', function() {
			let query = createQuery({
				animalId: { $exists: true }
			});
			expect(convertQuery(query, models.Animal)).to.deep.equal({
				exists: 'animalId'
			});
		});

		it('$exists: false', function() {
			let query = createQuery({
				animalId: { $exists: false }
			});
			expect(convertQuery(query, models.Animal)).to.deep.equal({
				missing: 'animalId'
			});
		});

		it('$in', function() {
			let query = createQuery({
				animalId: { $in: [ 'charles', 'barkley', 'baloo' ] }
			});
			expect(convertQuery(query, models.Animal)).to.deep.equal({
				terms: {
					animalId: [ 'charles', 'barkley', 'baloo' ]
				}
			});
		});

		it('$all', function() {
			let query = createQuery({
				animalId: { $all: [ 'charles', 'barkley', 'baloo' ] }
			});
			expect(convertQuery(query, models.Animal)).to.deep.equal({
				bool: { must: [
					{ term: { animalId: 'charles' } },
					{ term: { animalId: 'barkley' } },
					{ term: { animalId: 'baloo' } }
				] }
			});
		});

		it('$regex', function() {
			let query = createQuery({
				description: {
					$regex: 'dog'
				}
			});
			expect(convertQuery(query, models.Animal)).to.deep.equal({
				regexp: {
					description: 'dog'
				}
			});
		});

		it('$gt', function() {
			let query = createQuery({
				animalId: { $gt: 'dog' }
			});
			expect(convertQuery(query, models.Animal)).to.deep.equal({
				range: {
					animalId: {
						gt: 'dog'
					}
				}
			});
		});

		it('$gte', function() {
			let query = createQuery({
				animalId: { $gte: 'dog' }
			});
			expect(convertQuery(query, models.Animal)).to.deep.equal({
				range: {
					animalId: {
						gte: 'dog'
					}
				}
			});
		});

		it('$lt', function() {
			let query = createQuery({
				animalId: { $lt: 'dog' }
			});
			expect(convertQuery(query, models.Animal)).to.deep.equal({
				range: {
					animalId: {
						lt: 'dog'
					}
				}
			});
		});

		it('$lte', function() {
			let query = createQuery({
				animalId: { $lte: 'dog' }
			});
			expect(convertQuery(query, models.Animal)).to.deep.equal({
				range: {
					animalId: {
						lte: 'dog'
					}
				}
			});
		});

		it('$nin', function() {
			let query = createQuery({
				animalId: { $nin: [ 'charles', 'barkley', 'baloo' ] }
			});
			expect(convertQuery(query, models.Animal)).to.deep.equal({
				bool: { 'must_not': [
					{ term: { animalId: 'charles' } },
					{ term: { animalId: 'barkley' } },
					{ term: { animalId: 'baloo' } }
				] }
			});
		});

		it('$elemMatch', function() {
			let query = createQuery({
				beds: { $elemMatch: {
					bedId: 'couch'
				} }
			});
			expect(convertQuery(query, models.Animal)).to.deep.equal({
				nested: {
					path: 'beds',
					filter: { term: { bedId: 'couch' } }
				}
			});
		});

		it('$near GeoJSON', function() {
			let query = createQuery({
				loc: {
					$near: {
						$geometry: {
							type: 'Point',
							coordinates: [ 0.5, 1.3 ]
						},
						$maxDistance: 5
					}
				}
			});
			expect(convertQuery(query, models.Animal)).to.deep.equal({
				'geo_distance': {
					distance: '' + 5 + 'm',
					loc: [ 0.5, 1.3 ]
				}
			});
		});

	});

});
