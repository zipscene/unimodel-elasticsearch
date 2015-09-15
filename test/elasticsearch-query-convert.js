const { expect } = require('chai');
const commonQuery = require('zs-common-query');

const { queryConvert } = require('../lib/elasticsearch-query-convert');
const testUtils = require('./lib/test-utils');

describe('query-convert', function() {

	describe('query expressions', function() {

		it('$and', function() {
		});

		it('$nor', function() {
		});

		it('$or', function() {
		});

		it('$child', function() {
		});

		it('$parent', function() {
		});

		it('$child with minChildren', function() {
		});

		it('$child with maxChildren', function() {
		});

		it('should convert an empty query', function() {
		});

		it('should convert a single andFilter into an exact match', function() {
		});

		it('should convert a single orFilterSet into a should', function() {
		});

		it('should convert multiple orFilterSets into multiple bool shoulds', function() {
		});

		it('should support a complex combination', function() {
		});

		it('should fail $child/$parent if invalid minChildren/maxChildren is given', function() {
		});

		it('should fail $child/$parent if unknown Child/Parent is given', function() {
		});

		it('should bubble $child/$parent errros', function() {
		});

		it('should bubble operator expression errors', function() {
		});

	});

	describe('operators expressions', function() {

		it('$text', function() {
		});

		it('$wildcard', function() {
		});

		it('$not', function() {
		});

		it('$exists: true', function() {
		});

		it('$exists: false', function() {
		});

		it('$in', function() {
		});

		it('$all', function() {
		});

		it('$regex', function() {
		});

		it('$gt', function() {
		});

		it('$gte', function() {
		});

		it('$lt', function() {
		});

		it('$lte', function() {
		});

		it('$nin', function() {
		});

		it('$elemMatch', function() {
		});

		it('$near legacy', function() {
		});

		it('$near GeoJSON', function() {
		});

		it('should fail on unknown expressions', function() {
		});

		it('should fail on unindexed fields', function() {
		});

		it('should fail $text for non analyzed fields', function() {
		});

		it('should bubble $elemMatch nested failures', function() {
		});

		it('should fail $elemMatch if schema is not nested', function() {
		});

		it('should fail $near if invalid query type is given', function() {
		});

		it('should fail $near if no maxDistance is given', function() {
		});

	});

});
