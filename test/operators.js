// Copyright 2016 Zipscene, LLC
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

const { expect } = require('chai');
const { QueryValidationError, QueryFactory } = require('common-query');

const testUtils = require('./lib/test-utils');
const QueryOperatorParent = require('../lib/operators/parent');
const QueryOperatorChild = require('../lib/operators/child');
const queryFactory = new QueryFactory();
queryFactory.registerQueryOperator('$parent', new QueryOperatorParent());
queryFactory.registerQueryOperator('$child', new QueryOperatorChild());

describe('Query Operator', function() {

	let models;
	before(function() {
		this.timeout(0);
		return testUtils.resetAndConnect()
			.then(() => {
				models = testUtils.createTestModels();
			});
	});

	describe('$parent', function() {
		it('takes valid related queries', function() {
			let raw = { $parent: {
				ShelteredAnimal: {
					animalId: 'charles-barkley-dog'
				}
			} };
			expect(() => queryFactory.createQuery(raw, { schema: models.Shelter.getSchema() }))
				.to.not.throw();
		});

		it('should fail when unknown model is encountered', function() {
			let raw = { $parent: {
				UnknownModel: {}
			} };
			expect(() => queryFactory.createQuery(raw, { schema: models.Shelter.getSchema() }))
				.to.throw(QueryValidationError, 'Related model in $parent must be globally registered');
		});

		it('should fail when unexpected query operator exists', function() {
			let raw = { $parent: {
				$unknownOp: 5
			} };
			expect(() => queryFactory.createQuery(raw, { schema: models.Shelter.getSchema() }))
				.to.throw(QueryValidationError, 'Unexpected query operator $parent value: $unknownOp');
		});

		it('should fail if an invalid type is given', function() {
			let raw = { $parent: new Date() };
			expect(() => queryFactory.createQuery(raw, { schema: models.Shelter.getSchema() }))
				.to.throw(QueryValidationError, 'Argument to $parent must be a plain object');
		});

		it('normalizese the inner query', function() {
			let raw = { $parent: { ShelteredAnimal: {
				age: '5'
			} } };
			let query = queryFactory.createQuery(raw, { schema: models.Shelter.getSchema() });
			expect(query.getData()).to.deep.equal({ $parent: { ShelteredAnimal: {
				age: 5
			} } });
		});

		it('validates the inner query', function() {
			let raw = { $parent: { ShelteredAnimal: {
				age: { '$what?': 5 }
			} } };
			let query = queryFactory.createQuery(raw, { skipValidate: true });
			expect(() => query.validate())
				.to.throw(QueryValidationError, 'Unrecognized expression operator: $what?');
		});

		it('should fail normalization of inner query', function() {
			let raw = { $parent: { ShelteredAnimal: {
				age: 'yatterman'
			} } };
			expect(() => queryFactory.createQuery(raw, { schema: models.Shelter.getSchema() }))
				.to.throw(QueryValidationError, 'Invalid value at query path: age');
		});

	});

	describe('$child', function() {

		it('should allow $minChildren/$maxChildren', function() {
			let raw = { $child: {
				$minChildren: 5,
				$maxChildren: 10,
				ShelteredAnimal: {}
			} };
			let query = queryFactory.createQuery(raw, { schema: models.Shelter.getSchema() });
			expect(query.getData()).to.deep.equal(raw);
		});

		it('should normalize $minChilren/$maxChildren', function() {
			let raw = { $child: {
				$minChildren: '5',
				ShelteredAnimal: {}
			} };
			let query = queryFactory.createQuery(raw, { schema: models.Shelter.getSchema() });
			expect(query.getData()).to.deep.equal({
				$child: {
					$minChildren: 5,
					ShelteredAnimal: {}
				}
			});
		});

		it('should fail on unparsable $minChildren/$maxChildren', function() {
			let raw = { $child: {
				$minChildren: 'yatterman',
				ShelteredAnimal: {}
			} };
			expect(() => queryFactory.createQuery(raw, { schema: models.Shelter.getSchema() }))
				.to.throw(QueryValidationError, 'Query operator option $minChildren must be a number');
		});

		it('should validate $minChildren < $maxChildren', function() {
			let raw = { $child: {
				$minChildren: 15,
				$maxChildren: 10,
				ShelteredAnimal: {}
			} };
			expect(() => queryFactory.createQuery(raw, { schema: models.Shelter.getSchema() }))
				.to.throw(QueryValidationError, 'value of $minChildren must not be greater than value of $maxChildren');
		});

	});

});
