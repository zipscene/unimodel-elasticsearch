const { expect } = require('chai');
const { createQuery } = require('zs-common-query');

const { elasticsearchQueryConvert: queryConvert } = require('../lib/elasticsearch-query-convert');
const testUtils = require('./lib/test-utils');

describe('query-convert', function() {

	let models;
	before(() => testUtils.resetAndConnect().then(() => {
		models = testUtils.createTestModels();
	}));

	describe('query expressions', function() {

		it('should convert an empty query', function() {
			let query = createQuery({});
			expect(queryConvert(query, models.Animal)).to.deep.equal({ 'match_all': {} });
		});

		it('should convert an exact match', function() {
			let query = createQuery({ animalId: 'charles-barkley-dog-male' });
			expect(queryConvert(query, models.Animal))
				.to.deep.equal({ term: { animalId: 'charles-barkley-dog-male' } });
		});

		it('$and', function() {
			let query = createQuery({
				$and: [
					{ animalId: 'charles-barkley-dog-male' },
					{ animalId: 'baloo-dog-male' }
				]
			});
			expect(queryConvert(query, models.Animal)).to.deep.equal({
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
			expect(queryConvert(query, models.Animal)).to.deep.equal({
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
			expect(queryConvert(query, models.Animal)).to.deep.equal({
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
			expect(queryConvert(query, models.Animal)).to.deep.equal({
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

		it.skip('$child', function() {
			let query = createQuery({
				$child: {
					ShelteredAnimal: {
						animalId: 'charles-barkley-dog-male'
					}
				}
			});
			expect(queryConvert(query, models.Shelter)).to.deep.equal({
				bool: { must: [
					{ 'has_child': {
						type: 'ShelteredAnimal',
						filter: { bool: { term: {
							animalId: 'charles-barkley-dog-male'
						} } }
					} }
				] }
			});
		});

		it.skip('$parent', function() {
		});

		it.skip('$child with minChildren', function() {
		});

		it.skip('$child with maxChildren', function() {
		});

		it.skip('should support a complex combination', function() {
		});

		it.skip('should fail $child/$parent if invalid minChildren/maxChildren is given', function() {
		});

		it.skip('should fail $child/$parent if unknown Child/Parent is given', function() {
		});

		it.skip('should bubble $child/$parent errros', function() {
		});

	});

	describe('operators expressions', function() {

		it('$text', function() {
			let query = createQuery({
				description: {
					$text: 'dog'
				}
			});
			expect(queryConvert(query, models.Animal)).to.deep.equal({
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
			expect(queryConvert(query, models.Animal)).to.deep.equal({
				regexp: {
					description: '^dog.*$'
				}
			});
		});

		it('$not', function() {
			let query = createQuery({
				description: { $not: { $wildcard: 'dog' } }
			});
			expect(queryConvert(query, models.Animal)).to.deep.equal({
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
			expect(queryConvert(query, models.Animal)).to.deep.equal({
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
			expect(queryConvert(query, models.Animal)).to.deep.equal({
				exists: 'animalId'
			});
		});

		it('$exists: false', function() {
			let query = createQuery({
				animalId: { $exists: false }
			});
			expect(queryConvert(query, models.Animal)).to.deep.equal({
				missing: 'animalId'
			});
		});

		it('$in', function() {
			let query = createQuery({
				animalId: { $in: [ 'charles', 'barkley', 'baloo' ] }
			});
			expect(queryConvert(query, models.Animal)).to.deep.equal({
				terms: {
					animalId: [ 'charles', 'barkley', 'baloo' ]
				}
			});
		});

		it.skip('$all', function() {
			let query = createQuery({
				animalId: { $all: [ 'charles', 'barkley', 'baloo' ] }
			});
			expect(queryConvert(query, models.Animal)).to.deep.equal({
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
			expect(queryConvert(query, models.Animal)).to.deep.equal({
				regexp: {
					description: 'dog'
				}
			});
		});

		it('$gt', function() {
			let query = createQuery({
				animalId: { $gt: 'dog' }
			});
			expect(queryConvert(query, models.Animal)).to.deep.equal({
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
			expect(queryConvert(query, models.Animal)).to.deep.equal({
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
			expect(queryConvert(query, models.Animal)).to.deep.equal({
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
			expect(queryConvert(query, models.Animal)).to.deep.equal({
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
			expect(queryConvert(query, models.Animal)).to.deep.equal({
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
			expect(queryConvert(query, models.Animal)).to.deep.equal({
				nested: {
					path: 'beds',
					filter: { term: { bedId: 'couch' } }
				}
			});
		});

		it.skip('$near GeoJSON', function() {
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
			expect(queryConvert(query, models.Animal)).to.deep.equal({
				'geo_distance': {
					distance: '' + 5 + 'm',
					loc: [ 0.5, 1.3 ]
				}
			});
		});

	});

});
