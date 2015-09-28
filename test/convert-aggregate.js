const { expect } = require('chai');
const { createAggregate } = require('zs-common-query');
const moment = require('moment');

const testUtils = require('./lib/test-utils');
const { convertAggregate, convertAggregateResult } = require('../lib/convert/aggregate');

describe('Aggregates', function() {

	let models;
	before(() => testUtils.resetAndConnect()
		.then(() => {
			models = testUtils.createTestModels();
		}));

	describe('Aggregate Conversion', function() {

		it('should convert stats only aggregate', function() {
			let aggr = createAggregate({
				stats: {
					age: {
						count: true
					}
				}
			}, { schema: models.ShelteredAnimal.getSchema() });
			expect(convertAggregate(aggr)).to.deep.equal({
				global: {},
				aggregations: {
					'|age': {
						stats: {
							field: 'age'
						}
					}
				}
			});
		});

		it('should convert groupBy formats', function() {
			let dateJan = moment('2015-01-01', 'YYYY-MM-DD').toDate();
			let dateFeb = moment('2015-02-01', 'YYYY-MM-DD').toDate();
			let aggr = createAggregate({
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
						interval: 'P1D',
						base: dateFeb
					},
					{ // Numeric Interval
						field: 'age',
						interval: 2,
						base: 1
					},
					{ // Time Component
						field: 'found',
						timeComponent: 'day',
						timeComponentCount: 2
					}
				]
			}, { schema: models.ShelteredAnimal.getSchema() });
			let actual = convertAggregate(aggr);
			let expected = {
				terms: {
					field: 'name'
				},
				aggregations: {
					'date_range': {
						field: 'found',
						ranges: [
							{ to: dateJan.toISOString() },
							{ from: dateJan.toISOString(), to: dateFeb.toISOString() },
							{ from: dateFeb.toISOString() }
						],
						format: 'YYYY-MM-DDTHH:mm:ss.sssZ'
					},
					aggregations: {
						range: {
							field: 'age',
							ranges: [
								{ to: 2 },
								{ from: 2, to: 5 },
								{ from: 5 }
							]
						},
						aggregations: {
							'date_histogram': {
								field: 'found',
								interval: `${3600 * 24}s`, // One day
								'extended_bounds': {
									min: dateFeb.toISOString()
								}
							},
							aggregations: {
								histogram: {
									field: 'age',
									interval: 2,
									'extended_bounds': {
										min: 1
									}
								},
								aggregations: {
									'date_histogram': {
										field: 'found',
										interval: 'day'
									},
									aggregations: {
										'|age': {
											stats: { field: 'age' }
										}
									}
								}
							}
						}
					}
				}
			};
			expect(actual).to.deep.equal(expected);
		});

	});

	describe.skip('Aggregate Result Conversion', function() {

		it('should convert basic aggregate results', function() {

		});

	});

});


