const { expect } = require('chai');
const { createAggregate } = require('zs-common-query');
const moment = require('moment');

const testUtils = require('./lib/test-utils');
const { convertAggregate, convertAggregateResult } = require('../lib/convert/aggregate');
const joda = require('../lib/convert/joda');

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
					aggregate: {
						'date_range': {
							field: 'found',
							ranges: [
								{ to: joda.toJodaFormat(dateJan) },
								{ from: joda.toJodaFormat(dateJan), to: joda.toJodaFormat(dateFeb) },
								{ from: joda.toJodaFormat(dateFeb) }
							],
							format: 'yyyy-MM-ddHH:mm:ss.SSS'
						},
						aggregations: {
							aggregate: {
								range: {
									field: 'age',
									ranges: [
										{ to: 2 },
										{ from: 2, to: 5 },
										{ from: 5 }
									]
								},
								aggregations: {
									aggregate: {
										'date_histogram': {
											field: 'found',
											interval: `${3600 * 24}s`, // One day
											'extended_bounds': {
												min: joda.toJodaFormat(dateFeb)
											},
											format: joda.JODA_ISO_STRING_FORMAT
										},
										aggregations: {
											aggregate: {
												histogram: {
													field: 'age',
													interval: 2,
													'extended_bounds': {
														min: 1
													}
												},
												aggregations: {
													aggregate: {
														'date_histogram': {
															field: 'found',
															interval: '2d'
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
								}
							}
						}
					}
				}
			};
			expect(actual).to.deep.equal(expected);
		});

	});

	describe('Aggregate Result Conversion', function() {

		it('should convert aggregate results', function() {
			let aggregate = createAggregate({
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
			}, models.Animal.getSchema());
			let aggrResult = {
				'buckets': [
					{
						'key': 'charles',
						'doc_count': 2,
						'aggregate': {
							'buckets': [
								{
									'key': '*-2.0',
									'to': 2,
									'to_as_string': '2.0',
									'doc_count': 2
								},
								{
									'key': '2.0-5.0',
									'from': 2,
									'from_as_string': '2.0',
									'to': 5,
									'to_as_string': '5.0',
									'doc_count': 0
								},
								{
									'key': '5.0-*',
									'from': 5,
									'from_as_string': '5.0',
									'doc_count': 0
								}
							]
						}
					},
					{
						'key': 'ein',
						'doc_count': 2,
						'aggregate': {
							'buckets': [
								{
									'key': '*-2.0',
									'to': 2,
									'to_as_string': '2.0',
									'doc_count': 0
								},
								{
									'key': '2.0-5.0',
									'from': 2,
									'from_as_string': '2.0',
									'to': 5,
									'to_as_string': '5.0',
									'doc_count': 1
								},
								{
									'key': '5.0-*',
									'from': 5,
									'from_as_string': '5.0',
									'doc_count': 1
								}
							]
						}
					},
					{
						'key': 'baloo',
						'doc_count': 1,
						'aggregate': {
							'buckets': [
								{
									'key': '*-2.0',
									'to': 2,
									'to_as_string': '2.0',
									'doc_count': 0
								},
								{
									'key': '2.0-5.0',
									'from': 2,
									'from_as_string': '2.0',
									'to': 5,
									'to_as_string': '5.0',
									'doc_count': 1
								},
								{
									'key': '5.0-*',
									'from': 5,
									'from_as_string': '5.0',
									'doc_count': 0
								}
							]
						}
					}
				]
			};

			let commonResult = convertAggregateResult(aggrResult, aggregate);
			expect(commonResult).to.be.instanceof(Array);
			expect(commonResult).to.have.length(4);
			expect(commonResult).to.deep.include.members([
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


