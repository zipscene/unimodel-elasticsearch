const XError = require('xerror');

const ElasticsearchConnection = exports.ElasticsearchConnection = require('./elasticsearch-connection');
exports.ElasticsearchDocument = require('./elasticsearch-document');
exports.ElasticsearchError = require('./elasticsearch-error');
exports.ElasticsearchIndexManager = require('./elasticsearch-index-manager');
exports.ElasticsearchIndex = require('./elasticsearch-index');
const ElasticsearchModel = exports.ElasticsearchModel = require('./elasticsearch-model');

let defaultConnection = new ElasticsearchConnection();

// Expose the default connection and a connect method

exports.defaultConnection = defaultConnection;

exports.connect = function(url, indexConfigs/*, options = {}*/) {
	defaultConnection.setUrl(url);
	defaultConnection.setIndexConfigs(indexConfigs);
	defaultConnection.initialize();
	return defaultConnection;
};

// Add a function similar to mongoose which registers and retrieves models
let modelRegistry = {};
exports.model = function(name, model) {
	if (model) {
		modelRegistry[name] = model;
	} else {
		model = modelRegistry[model];
		if (!model) throw new XError(XError.INTERNAL_ERROR, `Model not found: ${name}`);
		return model;
	}
};

// Add a function to create models with the default connection
exports.createModel = function(typeName, schema, indexName, options = {}) {
	return new ElasticsearchModel(typeName, schema, indexName, defaultConnection, options);
};



