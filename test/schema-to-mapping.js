const { createSchema } = require('zs-common-schema');
const { expect } = require('chai');
const XError = require('xerror');

const { schemaToMapping } = require('../lib/schema-to-mapping');

describe('schema-to-mapping', function() {

	describe('type: string', function() {

		it('should convert basic string schema types', function() {
			let schema = createSchema({
				foo: String
			});
			let mapping = schemaToMapping(schema);
			let expected = {
				_all: {
					enabled: false
				},
				type: 'object',
				dynamic: false,
				properties: {
					foo: {
						type: 'string',
						index: 'no',
						null_value: undefined //eslint-disable-line camelcase
					}
				}
			};
			expect(mapping).to.deep.equals(expected);
		});

		it('should convert analyzed string schema types', function() {
			let schema = createSchema({
				foo: {
					type: String,
					index: 'analyzed',
					analyzer: 'standard'
				}
			});
			let mapping = schemaToMapping(schema);
			let expected = {
				_all: {
					enabled: false
				},
				type: 'object',
				dynamic: false,
				properties: {
					foo: {
						type: 'string',
						index: 'analyzed',
						analyzer: 'standard',
						null_value: undefined //eslint-disable-line camelcase
					}
				}
			};
			expect(mapping).to.deep.equals(expected);
		});

		it('should fail to convert analyzed string if no analyzer is specified', function() {
			let schema = createSchema({
				foo: {
					type: String,
					index: 'analyzed'
				}
			});
			expect(() => schemaToMapping(schema))
				.to.throw(XError, /Analyzed value at "foo" is missing analyzer value/);
		});

	});

	describe('type: number', function() {

		it('should convert number schema types', function() {
		});

	});

	describe('type: boolean', function() {

		it('should convert boolean schema types', function() {
		});

	});

	describe('type: date', function() {

		it('should convert date schema types', function() {
		});

	});

	describe('type: geopoint', function() {

		it('should convert geopoint schema types', function() {
		});

	});

	describe('type: object', function() {

		it('should convert object schema types', function() {
		});

	});

	describe('type: array', function() {

		it('should convert array schema types', function() {
		});

	});

	describe('type: map', function() {

		it('should convert map schema types', function() {
		});

	});

	describe('type: or', function() {

		it('should convert or schema types', function() {
		});

	});

	describe('type: mixed', function() {

		it('should convert mixed schema types', function() {
		});

	});

	describe('extra indexes', function() {

		it('should convert extra indexes into multifields', function() {
		});

		it('should set default index to an unnamed extra index', function() {
		});

		it('should fail if unexpected unnamed extra indexes are provided with no index', function() {
		});

		it('should fail if multiple extra indexes try to use the same name', function() {
		});

	});

	describe('general', function() {

		it('should convert a schema to an ElasticSearch mapping', function() {
			let schema = createSchema({
				lol: [ {
					foo: {
						type: String,
						index: true,
						default: 'nothing'
					},
					bar: {
						baz: {
							type: Number,
							numberType: 'float',
							precisionStep: 16
						}
					}
				} ]
			});

			let expected = {
				_all: {
					enabled: false
				},
				type: 'object',
				dynamic: false,
				properties: {
					lol: {
						type: 'object',
						dynamic: false,
						properties: {
							foo: {
								type: 'string',
								index: 'not_analyzed',
								null_value: 'nothing' //eslint-disable-line camelcase
							},
							bar: {
								type: 'object',
								dynamic: false,
								properties: {
									baz: {
										type: 'float',
										index: 'no',
										null_value: undefined, //eslint-disable-line camelcase
										precision_step: 16 //eslint-disable-line camelcase
									}
								}
							}
						}
					}
				}
			};

			let mapping = schemaToMapping(schema);
			expect(mapping).to.deep.equal(expected);
		});

		it('should convert complex combinations of schema types', function() {
		});

		it('should fail to if root of schema is not an object', function() {
		});

		it('should fail if unknown schema type is given', function() {
		});

	});

});
