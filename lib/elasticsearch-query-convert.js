const _ = require('lodash');
const {
	QueryValidationError,
	Query,
	coreExprOperators: { ExprOperatorWildcard }
} = require('zs-common-query');


function operatorExpressionToFilters(query, model, resultKeyPrefix, queryKey, operator) {
	let andFilters = [];
	let rangeParams = {};
	let errors = [];
	for (let exprKey in operator) {
		let exprVal = operator[exprKey];
		let esFilter;
		try {
			if (exprKey === '$text') {
				esFilter = { query: { match: {
					[`${resultKey(resultKeyPrefix, ensureTextMatchField(model, queryKey))}`]: {
						query: exprVal,
						operator: 'and'
					}
				} } };

			} else if (exprKey === '$wildcard') {
				let wildcardRegex = ExprOperatorWildcard.makeWildcardRegex(exprVal);
				esFilter = { regexp: {
					[`${resultKey(resultKeyPrefix, ensureExactMatchField(model, queryKey))}`]: wildcardRegex.source
				} };

			} else if (exprKey === '$not') {
				let [ nottedExprs, nottedErrors ] = operatorExpressionToFilters(
					query,
					model,
					resultKeyPrefix,
					queryKey,
					exprVal
				);
				if (nottedErrors && nottedErrors.length) {
					errors.push(...nottedErrors);
					continue;
				}
				esFilter = { bool: {
					'must_not': (nottedExprs.length === 1) ? nottedExprs[0] : nottedExprs
				} };

			} else if (exprKey === '$exists') {
				let esExists = exprVal ? 'exists' : 'missing';
				esFilter = { [`${esExists}`]: resultKey(resultKeyPrefix, ensureExactMatchField(model, queryKey)) };

			} else if (exprKey === '$in') {
				esFilter = { terms: {
					[`${resultKey(resultKeyPrefix, ensureExactMatchField(model, queryKey))}`]: exprVal
				} };
			} else if (exprKey === '$all') {
				if (!Array.isArray(exprVal)) {
					throw new QueryValidationError('$all value must be an Array');
				}

				esFilter = { bool: { must: [] } };
				for (let item of exprVal) {
					esFilter.bool.must.push({
						term: {
							[`${resultKey(resultKeyPrefix, ensureExactMatchField(model, queryKey))}`]: item
						}
					});
				}

			} else if (exprKey === '$regex') {
				let regex = exprVal instanceof RegExp ? exprVal.source : exprVal;
				esFilter = { regexp: {
					[`${resultKey(resultKeyPrefix, ensureExactMatchField(model, queryKey))}`]: regex
				} };

			} else if (exprKey === '$gt') {
				rangeParams.gt = exprVal;

			} else if (exprKey === '$gte') {
				rangeParams.gte = exprVal;

			} else if (exprKey === '$lt') {
				rangeParams.lt = exprVal;

			} else if (exprKey === '$lte') {
				rangeParams.lte = exprVal;

			} else if (exprKey === '$ne') {
				esFilter = {
					not: { filter: { term: {
						[`${resultKey(resultKeyPrefix, ensureExactMatchField(model, queryKey))}`]: exprVal
					} } }
				};

			} else if (exprKey === '$nin') {
				if (!Array.isArray(exprVal)) {
					throw new QueryValidationError('$nin value must be an Array');
				}
				let field = resultKey(resultKeyPrefix, ensureExactMatchField(model, queryKey));
				if (hasArrayParent(model, field)) {
					throw new QueryValidationError(`Field: ${queryKey} can\'t be part of an array`);
				}
				let mustNotNinFilters = [];
				for (let item of exprVal) {
					mustNotNinFilters.push({ term: {
						[`${field}`]: item
					} });
				}
				esFilter = { bool: { 'must_not': mustNotNinFilters } };

			} else if (exprKey === '$elemMatch') {
				let nestedSchemaData = model.getSchema()._createSubschema(model.getSchema().getSubschemaData(queryKey));
				if (!nestedSchemaData || !nestedSchemaData.getData().nested) {
					throw new QueryValidationError(`Field is not indexed for $elemMatch: ${queryKey}`);
				}
				let nestedResultKeyPrefix = resultKey(resultKeyPrefix, queryKey);
				let [ nestedFilter, nestedErrors ] = queryExpressionToFilter(exprVal, model, nestedResultKeyPrefix);
				if (nestedErrors && nestedErrors.length) {
					errors.push(...nestedErrors);
					continue;
				}
				esFilter = {
					nested: {
						path: nestedResultKeyPrefix,
						filter: nestedFilter
					}
				};

			} else if (exprKey === '$near') {
				// Support both the mongo geojson syntax and the "legacy coordinates" syntax
				let nearCoords;
				let nearMaxDistance;
				let isGeoJsonFormat = false;
				if (
					_.isArray(exprVal) && exprVal.length === 2 &&
						_.isNumber(exprVal[0]) && _.isNumber(exprVal[1])
				) {
					// Uses "legacy coordinates" syntax
					nearCoords = exprVal;

				} else if (
					exprVal && typeof exprVal === 'object' &&
						exprVal.$geometry && _.isPlainObject(exprVal.$geometry) &&
						exprVal.$geometry.type === 'Point' &&
						_.isArray(exprVal.$geometry.coordinates) &&
						exprVal.$geometry.coordinates.length === 2 &&
						_.isNumber(exprVal.$geometry.coordinates[0]) &&
						_.isNumber(exprVal.$geometry.coordinates[1])
				) {
					// Uses geojson syntax
					isGeoJsonFormat = true;
					nearCoords = exprVal.$geometry.coordinates;
				} else {
					throw new QueryValidationError('Invalid format for $near operator');
				}
				// Ensure $maxDistance is provided
				if (exprVal.$maxDistance || operator.$maxDistance) {
					nearMaxDistance = exprVal.$maxDistance || operator.$maxDistance;
				} else {
					throw new QueryValidationError('Must provide $maxDistance with $near operator');
				}
				if (_.isNumber(nearMaxDistance)) {
					if (isGeoJsonFormat) {
						// distance is in meters
						nearMaxDistance = '' + nearMaxDistance + 'm';
					} else {
						// distance is in radians
						nearMaxDistance = '' + Math.floor(nearMaxDistance * 6371000) + 'm';
					}
				} else if (_.isString(nearMaxDistance)) {
					throw new QueryValidationError('Invalid format for $maxDistance option');
				}
				esFilter = {
					'geo_distance': {
						distance: nearMaxDistance,
						[`${resultKey(resultKeyPrefix, ensureExactMatchField(model, queryKey))}`]: nearCoords
					}
				};

			} else if (exprKey !== '$options' && exprKey !== '$maxDistance') {
				throw new QueryValidationError(`Unknown query operator expression operator: ${exprKey}`);
			}
		} catch(err) {
			errors.push(err);
			continue;
		}
		if (esFilter) andFilters.push(esFilter);
	}
	if (!_.isEmpty(rangeParams)) {
		let rangeFilter = {
			[`${resultKey(resultKeyPrefix, ensureExactMatchField(model, queryKey))}`]: rangeParams
		};
		andFilters.push({ range: rangeFilter });
	}
	return [ andFilters, errors ];
}

function queryExpressionToFilter(query, model, resultKeyPrefix) {
	let andFilters = [];
	let orFilterSets = [];
	let norFilters = [];
	let errors = [];
	for (let key in query) {
		let queryVal = query[key];
		try {
			if (key[0] === '$') {
				// This is a query operator
				if (key === '$and' || key === '$or' || key === '$nor') {
					// Array query operators
					let destination;
					if (key === '$and') {
						destination = andFilters;
					} else if (key === '$nor') {
						destination = norFilters;
					} else { // $or
						// Need to create a new filterSet
						destination = [];
						orFilterSets.push(destination);
					}
					for (let subQuery of queryVal) {
						let [ subESQuery, subErrors ] = queryExpressionToFilter(subQuery, model, resultKeyPrefix);
						errors.push(...subErrors);
						destination.push(subESQuery);
					}
				} else if (key === '$child' || key === '$parent') {
					// Relationship query operators
					// Validate min/max children parameters
					let minChildren = ensureNumber(queryVal.$minChildren, query);
					let maxChildren = ensureNumber(queryVal.$maxChildren, query);
					if (_.isNumber(minChildren) && _.isNumber(maxChildren) && minChildren > maxChildren) {
						let msg = 'value of $minChildren must not be greater than value of $maxChildren';
						throw new QueryValidationError(msg, { query });
					}
					for (let childType in queryVal) {
						if (childType[0] === '$') {
							let msg = `Unexpected query operator in ${key} value: ${childType[0]}`;
							throw new QueryValidationError(msg, { query });
						}
						let childModel;
						try {
							childModel = require('./index').model(childType); // try to prevent circular dependency
						} catch (ex) {
							throw new QueryValidationError(ex.message, { query }, ex);
						}
						let childQuery = queryVal[childType];
						let [ childESQuery, childErrors ] = queryExpressionToFilter(childQuery, childModel);
						if (childErrors && childErrors.length) {
							errors.push(...childErrors);
							continue;
						}

						if (key === '$child') {
							let hasChildQuery = {
								type: childModel.getName(),
								filter: childESQuery
							};
							if (_.isNumber(minChildren)) {
								hasChildQuery.min_children = minChildren; //eslint-disable-line camelcase
							}
							if (_.isNumber(maxChildren)) {
								hasChildQuery.max_children = maxChildren; //eslint-disable-line camelcase
							}
							andFilters.push({ 'has_child': hasChildQuery });
						} else {
							andFilters.push({
								'has_parent': {
									type: childModel.getName(),
									filter: childESQuery
								}
							});
						}
					}
				} else {
					throw new QueryValidationError(`Unknown query operator: ${key}`);
				}
			} else if (Query.isOperatorExpression(queryVal)) {
				let [ opFilters, opErrors ] = operatorExpressionToFilters(query, model, resultKeyPrefix, key, queryVal);
				errors.push(...opErrors);
				andFilters.push(...opFilters);
			} else {
				// Is an exact match
				andFilters.push({ term: {
					[`${ensureExactMatchField(model, resultKey(resultKeyPrefix, key))}`]: queryVal
				} });
			}
		} catch(err) {
			errors.push(err);
			continue;
		}
	}

	// Don't bother building a query if there were errors
	if (errors.length) {
		return [ null, errors ];
	}
	// Put together the components for the actual elasticsearch query
	let esQuery;
	if (!andFilters.length && !norFilters.length && !orFilterSets.length) {
		esQuery = { 'match_all': {} };
	} else if (andFilters.length === 1 && !norFilters.length && !orFilterSets.length) {
		esQuery = andFilters[0];
	} else {
		let boolFilter = {};
		if (andFilters.length) {
			boolFilter.must = andFilters;
		}
		if (norFilters.length) {
			boolFilter.must_not = norFilters; //eslint-disable-line camelcase
		}
		if (orFilterSets.length === 1) {
			boolFilter.should = orFilterSets[0];
		} else if (orFilterSets.length) {
			if (!boolFilter.must) { boolFilter.must = []; }
			for (let orFilters of orFilterSets) {
				boolFilter.must.push({ bool: { should: orFilters } });
			}
		}
		esQuery = { bool: boolFilter };
	}
	// Return the query, along with any errors
	return [ esQuery, errors ];
}

/**
 * This converts a common-query query to an elasticsearch query.  It takes into account the
 * common-schema schema for the type.
 *
 * TODO: handle extra indexes
 * TODO: handle $elemMatch requiring a nested object
 *
 * @method elasticsearchQueryConvert
 * @throws {QueryValidationError} - When query cannot be converted or does not match the schema
 * @param {Query} query - The common-query object
 * @param {ElasticsearchModel} model - The zs-unimodel-elasticsearch object holding the schema and indexes
 * @return {Object} - The raw elasticsearch query
 */
function elasticsearchQueryConvert(query, model) {
	// Need to traverse the query data ourselves properly build the ESQuery
	let queryData = query.getData();
	let [ filter, errors ] = queryExpressionToFilter(queryData, model);
	if (errors && errors.length) {
		if (errors.length === 1) {
			// Just return the only error
			throw errors[0];
		}
		// Build multiple errors object
		throw new QueryValidationError(errors[0].message + `...+${errors.length - 1} more`, errors);
	}
	return filter;
}

exports.elasticsearchQueryConvert = elasticsearchQueryConvert;


/// Helper functions

/**
 * Make sure the value is either a number or undefined
 */
function ensureNumber(val, query) {
	if (_.isString(val)) {
		val = parseFloat(val);
		if (isNaN(val)) {
			let msg = `Cannot parse string ${val} into a number`;
			throw new QueryValidationError(msg, { query: query });
		}
	} else if (!_.isNumber(val)) {
		val = undefined;
	}
	return val;
}

/**
 * Ensure the field is indexed for "text" matches (ie "analyzed")
 */
function ensureTextMatchField(model, field) {
	let indexes = getFieldIndexes(model, field);
	if (!indexes || _.contains(indexes, 'analyzed')) {
		throw new QueryValidationError(`Field is indexed for text match: ${field}`);
	}
	return field;
}

/**
 * Ensure the field is indexed for "exact" matches (ie any truthy "index" property)
 */
function ensureExactMatchField(model, field) {
	if (field === '_parent') return field;	// special case, automatically indexed
	let indexes = getFieldIndexes(model, field);
	if (!indexes || !indexes.length) {
		throw new QueryValidationError(`Field is not indexed: ${field}`);
	}
	return field;
}

/**
 * Get the key for this field, applying a prefix, if needed
 */
function resultKey(prefix, key) {
	return prefix ? (prefix + '.' + key) : key;
}

/**
 * Get indexes for a given field
 */
function getFieldIndexes(model, field) {
	let fieldIndexes = [];
	try {
		// Get the index from the schema data
		let defaultIndex = model.getSchema().getSubschemaData(field).index;
		if (defaultIndex !== undefined) {
			fieldIndexes.push(defaultIndex);
		}
	} catch(err) {
		throw new QueryValidationError(`Could not find field ${field} in schema`, { field }, err);
	}

	// Find any extra indexes referencing this field
	for (let index of model.extraIndexes) {
		if (index.field === field) {
			fieldIndexes.push(index.index);
		}
	}

	return fieldIndexes;
}

/**
 * Check if path has a parent, which is an array
 */
function hasArrayParent(model, queryKey) {
	let schema = model.getSchema();
	let fieldParts = queryKey.split('.');
	let field = '';
	try {
		for (let fieldPart of fieldParts) {
			field = (field) ? `${field}.${fieldPart}` : fieldPart;
			let subschemaData = schema.getSubschemaData(field);
			// accessing field within an array
			if (subschemaData.type === 'array') {
				return true;
			}
		}
	} catch(err) {
		// Return true in this failure case
		return true;
	}
	// arrived at field without going through array
	return false;
}
