// Copyright 2016 Zipscene, LLC
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

const { createSchema, map, or, Mixed } = require('common-schema');
const { expect } = require('chai');

const { convertSchema } = require('../lib/convert/schema');
const { ElasticsearchMappingValidationError } = require('../lib');

describe('convertSchema', function() {

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
				_all: { enabled: false },
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

			let mapping = convertSchema(schema);
			expect(mapping).to.deep.equal(expected);
		});

		it('should fail to if root of schema is not an object', function() {
			let schema = createSchema([ Number ]);
			expect(() => convertSchema(schema))
				.to.throw(
					ElasticsearchMappingValidationError,
					/Schema root must be type "object" to be converted to ElasticSearch Mapping/
				);
		});

		it('should fail if unknown schema type is given', function() {
			let schema = createSchema({
				foo: String
			});
			schema.getData().properties.foo.type = 'invalid!';
			expect(() => convertSchema(schema))
				.to.throw(
					ElasticsearchMappingValidationError,
					/Cannot convert unknown schema type \(invalid!\) to ElasticSearch Mapping/
				);
		});

	});

	describe('type: string', function() {

		it('should convert basic string schema types', function() {
			let schema = createSchema({
				foo: String
			});
			let mapping = convertSchema(schema);
			let expected = {
				_all: { enabled: false },
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
			let mapping = convertSchema(schema);
			let expected = {
				_all: { enabled: false },
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
			expect(() => convertSchema(schema))
				.to.throw(ElasticsearchMappingValidationError, /Analyzed value at "foo" is missing analyzer value/);
		});

	});

	describe('type: number', function() {

		it('should convert number schema types', function() {
			let schema = createSchema({
				bar: Number
			});
			let mapping = convertSchema(schema);
			let expected = {
				_all: { enabled: false },
				properties: {
					bar: {
						type: 'double',
						index: 'no',
						precision_step: 8, //eslint-disable-line camelcase
						null_value: undefined //eslint-disable-line camelcase
					}
				}
			};
			expect(mapping).to.deep.equals(expected);
		});

		it('should convert number schema types with custom numberType', function() {
			let schema = createSchema({
				bar: {
					type: Number,
					numberType: 'integer'
				}
			});
			let mapping = convertSchema(schema);
			let expected = {
				_all: { enabled: false },
				properties: {
					bar: {
						type: 'integer',
						index: 'no',
						precision_step: 8, //eslint-disable-line camelcase
						null_value: undefined //eslint-disable-line camelcase
					}
				}
			};
			expect(mapping).to.deep.equals(expected);
		});

		it('should convert number schema types with custom precisionStep', function() {
			let schema = createSchema({
				bar: {
					type: Number,
					precisionStep: 16
				}
			});
			let mapping = convertSchema(schema);
			let expected = {
				_all: { enabled: false },
				properties: {
					bar: {
						type: 'double',
						index: 'no',
						precision_step: 16, //eslint-disable-line camelcase
						null_value: undefined //eslint-disable-line camelcase
					}
				}
			};
			expect(mapping).to.deep.equals(expected);
		});

		it('should fail to convert number schema types with invalid numberType', function() {
			let schema = createSchema({
				bar: {
					type: Number,
					numberType: 'bigint'
				}
			});
			expect(() => convertSchema(schema))
				.to.throw(
					ElasticsearchMappingValidationError,
					/Number type at "bar" is an invalid ElasticSearch Number Type/
				);
		});

		it('should fail to convert number schema types with invalid precisionStep', function() {
			let schema = createSchema({
				bar: {
					type: Number,
					precisionStep: 'invalid'
				}
			});
			expect(() => convertSchema(schema))
				.to.throw(
					ElasticsearchMappingValidationError,
					/Number precision step at "bar" must be an integer or a parsable string/
				);
		});

	});

	describe('type: boolean', function() {

		it('should convert boolean schema types', function() {
			let schema = createSchema({
				bar: Boolean
			});
			let mapping = convertSchema(schema);
			let expected = {
				_all: { enabled: false },
				properties: {
					bar: {
						type: 'boolean',
						index: 'no',
						null_value: undefined //eslint-disable-line camelcase
					}
				}
			};
			expect(mapping).to.deep.equals(expected);
		});

	});

	describe('type: date', function() {

		it('should convert date schema types', function() {
			let schema = createSchema({
				bar: Date
			});
			let mapping = convertSchema(schema);
			let expected = {
				_all: { enabled: false },
				properties: {
					bar: {
						type: 'date',
						index: 'no',
						format: 'date_optional_time',
						null_value: undefined //eslint-disable-line camelcase
					}
				}
			};
			expect(mapping).to.deep.equals(expected);
		});

		it('should convert date schema types with custom format', function() {
			let schema = createSchema({
				bar: {
					type: Date,
					format: 'custom!'
				}
			});
			let mapping = convertSchema(schema);
			let expected = {
				_all: { enabled: false },
				properties: {
					bar: {
						type: 'date',
						index: 'no',
						format: 'custom!',
						null_value: undefined //eslint-disable-line camelcase
					}
				}
			};
			expect(mapping).to.deep.equals(expected);
		});

	});

	describe('type: geopoint', function() {

		it('should convert geopoint schema types', function() {
			let schema = createSchema({
				foo: {
					type: 'geopoint',
					index: true
				}
			});
			let mapping = convertSchema(schema);
			let expected = {
				_all: { enabled: false },
				properties: {
					foo: {
						type: 'geo_point',
						index: undefined,
						geohash: true,
						null_value: undefined //eslint-disable-line camelcase
					}
				}
			};
			expect(mapping).to.deep.equals(expected);
		});

		it('should convert unindexed geopoint schema to double', function() {
			let schema = createSchema({
				foo: 'geopoint'
			});
			let mapping = convertSchema(schema);
			let expected = {
				_all: { enabled: false },
				properties: {
					foo: {
						type: 'double',
						index: 'no',
						null_value: undefined //eslint-disable-line camelcase
					}
				}
			};
			expect(mapping).to.deep.equals(expected);
		});

		it('should convert geopoint schema types with no geohash', function() {
			let schema = createSchema({
				foo: {
					type: 'geopoint',
					index: true,
					geohash: false
				}
			});
			let mapping = convertSchema(schema);
			let expected = {
				_all: { enabled: false },
				properties: {
					foo: {
						type: 'geo_point',
						index: undefined,
						geohash: false,
						null_value: undefined //eslint-disable-line camelcase
					}
				}
			};
			expect(mapping).to.deep.equals(expected);
		});

	});

	describe('type: map', function() {

		it('should convert map schema types when indexed', function() {
			let schema = createSchema({
				foo: {
					type: 'map',
					values: {
						bar: String,
						baz: { type: String, index: true }
					}
				}
			});
			let mapping = convertSchema(schema);
			let expected = {
				properties: {
					foo: {
						type: 'object',
						dynamic: true
					}
				},
				_all: {
					enabled: false
				}
			};
			expect(mapping).to.deep.equals(expected);
		});

	});

	describe('type: object', function() {

		it('should convert object schema types', function() {
			let schema = createSchema({
				foo: {
					bar: String,
					baz: Date
				}
			});
			let mapping = convertSchema(schema);
			let expected = {
				_all: { enabled: false },
				properties: {
					foo: {
						type: 'object',
						dynamic: false,
						properties: {
							bar: {
								type: 'string',
								index: 'no',
								null_value: undefined //eslint-disable-line camelcase
							},
							baz: {
								type: 'date',
								index: 'no',
								format: 'date_optional_time',
								null_value: undefined //eslint-disable-line camelcase
							}
						}
					}
				}
			};
			expect(mapping).to.deep.equals(expected);
		});

	});

	describe('type: array', function() {

		it('should convert unindexed array schema types', function() {
			let schema = createSchema({
				foo: [ String ]
			});
			let mapping = convertSchema(schema);
			let expected = {
				_all: { enabled: false },
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

		it('should convert indexed array schema types', function() {
			let schema = createSchema({
				foo: {
					type: [ String ],
					index: true
				}
			});
			let mapping = convertSchema(schema);
			let expected = {
				_all: { enabled: false },
				properties: {
					foo: {
						type: 'string',
						index: 'not_analyzed',
						null_value: undefined //eslint-disable-line camelcase
					}
				}
			};
			expect(mapping).to.deep.equals(expected);
		});

		it('should convert indexed elements property in schema type', function() {
			let schema = createSchema({
				foo: {
					type: 'array',
					elements: {
						type: 'string',
						index: true
					}
				}
			});
			let mapping = convertSchema(schema);
			let expected = {
				_all: { enabled: false },
				properties: {
					foo: {
						type: 'string',
						index: 'not_analyzed',
						null_value: undefined //eslint-disable-line camelcase
					}
				}
			};
			expect(mapping).to.deep.equals(expected);
		});

	});

	describe('type: or', function() {

		it('should should always throw an error', function() {
			let schema = createSchema({
				foo: or({}, String, Number)
			});
			expect(() => convertSchema(schema))
				.to.throw(ElasticsearchMappingValidationError, /Invalid schema type provided: or/);
		});

	});

	describe('type: mixed', function() {

		it('should convert or schema types', function() {
			let schema = createSchema({
				foo: {
					type: Mixed,
					index: true
				}
			});
			let mapping = convertSchema(schema);
			let expected = {
				_all: { enabled: false },
				properties: {
					foo: {
						type: 'object',
						dynamic: true
					}
				}
			};
			expect(mapping).to.deep.equals(expected);
		});

		it('should convert unindexed or schema types', function() {
			let schema = createSchema({
				foo: Mixed
			});
			let mapping = convertSchema(schema);
			let expected = {
				_all: { enabled: false },
				properties: {
					foo: {
						type: 'object',
						dynamic: false,
						enabled: false
					}
				}
			};
			expect(mapping).to.deep.equals(expected);
		});

	});

	describe('extra indexes', function() {

		it('should convert extra indexes into multifields', function() {
			let schema = createSchema({
				foo: {
					type: String,
					index: false
				}
			});
			let mapping = convertSchema(schema, [
				{ field: 'foo', index: true, name: 'named' },
				{ field: 'foo', index: 'analyzed', analyzer: 'standard' },
				{ field: 'ignored', index: false }
			]);
			let expected = {
				_all: { enabled: false },
				properties: {
					foo: {
						type: 'string',
						index: 'no',
						null_value: undefined, //eslint-disable-line camelcase
						fields: {
							standard: {
								type: 'string',
								index: 'analyzed',
								analyzer: 'standard',
								null_value: undefined //eslint-disable-line camelcase
							},
							named: {
								type: 'string',
								index: 'not_analyzed',
								null_value: undefined //eslint-disable-line camelcase
							}
						}
					}
				}
			};
			expect(mapping).to.deep.equals(expected);
		});

		it('should set default index to an unnamed extra index', function() {
			let schema = createSchema({
				foo: String
			});
			let mapping = convertSchema(schema, [
				{ field: 'foo', index: true, name: 'named' },
				{ field: 'foo', index: 'analyzed', analyzer: 'standard' },
				{ field: 'ignored', index: false }
			]);
			let expected = {
				_all: { enabled: false },
				properties: {
					foo: {
						type: 'string',
						index: 'analyzed',
						analyzer: 'standard',
						null_value: undefined, //eslint-disable-line camelcase
						fields: {
							named: {
								type: 'string',
								index: 'not_analyzed',
								null_value: undefined //eslint-disable-line camelcase
							}
						}
					}
				}
			};
			expect(mapping).to.deep.equals(expected);
		});

		it('should fail if multiple unnamed extra indexes are provided with no index', function() {
			let schema = createSchema({
				foo: String
			});
			expect(() => convertSchema(schema, [
				{ field: 'foo', index: true },
				{ field: 'foo', index: 'analyzed', analyzer: 'standard' },
				{ field: 'ignored', index: false }
			])).to.throw(
				ElasticsearchMappingValidationError,
				/Multiple extra schemas tried to register as the default schema for "foo"/
			);
		});

		it('should fail if multiple extra indexes try to use the same name', function() {
			let schema = createSchema({
				foo: {
					type: String,
					index: false
				}
			});
			expect(() => convertSchema(schema, [
				{ field: 'foo', index: true, name: 'named' },
				{ field: 'foo', index: 'analyzed', analyzer: 'standard', name: 'named' },
				{ field: 'ignored', index: false }
			])).to.throw(
				ElasticsearchMappingValidationError,
				/Value mapping cannot have multiple subfields with the same name \(named\)/
			);
		});

		it('should fail if extra index is not applied to a value type', function() {
			let schema = createSchema({
				foo: {
					bar: String
				}
			});
			expect(() => convertSchema(schema, [
				{ field: 'foo', index: true }
			])).to.throw(
				ElasticsearchMappingValidationError,
				/Cannot apply extra index to a non-"value" schema type/
			);
		});

	});

	describe('_id', function() {

		it('should automatically set the _id field', function() {
			let schema = createSchema({
				foo: {
					type: String,
					id: true,
					index: true
				}
			});
			let mapping = convertSchema(schema);
			let expected = {
				_all: { enabled: false },
				_id: { path: 'foo' },
				properties: {
					foo: {
						type: 'string',
						index: 'not_analyzed',
						null_value: undefined //eslint-disable-line camelcase
					}
				}
			};
			expect(mapping).to.deep.equal(expected);
		});

	});

	describe('_all', function() {

		it('should respect includeAllField option', function() {
			let schema = createSchema({ foo: String });
			let mapping = convertSchema(schema, [], { includeAllField: true });
			let expected = {
				_all: { enabled: true },
				properties: {
					foo: {
						type: 'string',
						index: 'no',
						null_value: undefined //eslint-disable-line camelcase
					}
				}
			};
			expect(mapping).to.deep.equal(expected);
		});

	});

	describe('_parent', function() {

		it('should respect the parentType option', function() {
			let schema = createSchema({ foo: String });
			let mapping = convertSchema(schema, [], { parentType: 'Foobear' });
			let expected = {
				_all: { enabled: false },
				_parent: { type: 'Foobear' },
				properties: {
					foo: {
						type: 'string',
						index: 'no',
						null_value: undefined //eslint-disable-line camelcase
					}
				}
			};
			expect(mapping).to.deep.equal(expected);
		});

	});

});
