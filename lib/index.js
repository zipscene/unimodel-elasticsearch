// Copyright 2016 Zipscene, LLC
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

const XError = require('xerror');

// Reexport elasticsearch common query helpers
exports.commonQuery = require('./common-query');

// Reexport classes
const ElasticsearchConnection = exports.ElasticsearchConnection = require('./elasticsearch-connection');
exports.ElasticsearchDocument = require('./elasticsearch-document');
exports.ElasticsearchError = require('./elasticsearch-error');
exports.ElasticsearchIndexManager = require('./elasticsearch-index-manager');
exports.ElasticsearchIndex = require('./elasticsearch-index');
const ElasticsearchModel = exports.ElasticsearchModel = require('./elasticsearch-model');

// Reexport errors
exports.ElasticsearchMappingValidationError = require('./elasticsearch-mapping-validation-error');

// Reexport converters
const convert = require('./convert');
exports.convertQuery = convert.convertQuery;
exports.convertAggregate = convert.convertAggregate;
exports.convertAggregateResult = convert.convertAggregateResult;
exports.convertSchema = convert.convertSchema;

let defaultConnection = new ElasticsearchConnection({}, {}, {}, { initialize: false });

// Expose the default connection and a connect method

exports.defaultConnection = defaultConnection;

exports.connect = function(clientOptions={}, indexConfigs = {}, indexOptions={}) {
	defaultConnection.setIndexConfigs(indexConfigs);
	defaultConnection.setIndexOptions(indexOptions);
	defaultConnection.setClientOptions(clientOptions); // This will trigger `initialize`
	return defaultConnection;
};

// Add a function similar to mongoose which registers and retrieves models
let modelRegistry = {};
exports.model = function(name, model) {
	if (model) {
		modelRegistry[name] = model;
	} else {
		model = modelRegistry[name];
		if (!model) throw new XError(XError.INTERNAL_ERROR, `Model not found: ${name}`);
		return model;
	}
};

// Add a function to create models with the default connection
exports.createModel = function(typeName, schema, indexName, options = {}) {
	return new ElasticsearchModel(typeName, schema, indexName, defaultConnection, options);
};
