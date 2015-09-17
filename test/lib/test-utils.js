const XError = require('xerror');

const ElasticsearchConnection = require('../../lib').ElasticsearchConnection;
const ElasticsearchModel = require('../../lib').ElasticsearchModel;

const testConfig = {
	host: 'http://localhost:9200',
	indexConfigs: {
		'uetest_*': {
			shards: 16
		},
		'uitest_what': {
			shards: 5,
			replicas: 2
		}
	},
	allTestIndexes: 'uetest_*'
};
function getConfig() {
	return testConfig;
}
exports.getConfig = getConfig;

let testConnection;

function getConnection() {
	return testConnection;
}
exports.getConnection = getConnection;

function connect() {
	if (testConnection) {
		testConnection.close();
	}
	testConnection = new ElasticsearchConnection(testConfig.host, testConfig.indexConfigs);
	return testConnection.connectionWaiter.promise;
}

function resetData() {
	if (!testConnection) {
		return Promise.resolve();
	}
	return testConnection.getClient()
		.then((client) => client.indices.delete({ index: testConfig.allTestIndexes }))
		.catch((err) => {
			if (err.code === XError.NOT_FOUND) { return Promise.resolve(); }
			return Promise.reject(err);
		})
		.then(() => new Promise((resolve) => {
			// ElasticSearch doesn't necessarily delete the data right away, so we need to give it
			// some time to run the delete.
			setTimeout(resolve, 500);
		}));
}

function resetAndConnect() {
	return resetData()
		.then(connect)
		.then(resetData)
		.then(getConnection);
}

exports.resetAndConnect = resetAndConnect;


function createTestModels() {
	// Need to make these more complete
	let models = {

		Animal: new ElasticsearchModel('Animal', {
			animalId: { type: String, index: true, id: true, key: true },
			name: { type: String, index: true, key: true },
			sex: { type: String, enum: [ 'male', 'female', 'unknown' ] },
			description: { type: String, index: true },
			loc: { type: 'geojson', index: true },
			beds: {
				type: 'array',
				nested: true,
				index: true,
				elements: {
					bedId: { type: String, index: true }
				}
			}
		}, 'uetest_animals', testConnection),

		ShelteredAnimal: new ElasticsearchModel('ShelteredAnimal', {
			animalId: { type: String, index: true, id: true, key: true },
			name: { type: String, index: true, key: true },
			sex: { type: String, enum: [ 'male', 'female', 'unknown' ] },
			description: { type: String, index: true },
			found: { type: Date, index: true }
		}, 'uetest_shelters', testConnection, { parentType: 'Shelter' }),

		Shelter: new ElasticsearchModel('Shelter', {
			shelterId: { type: String, index: true, id: true, key: true },
			name: { type: String, key: true }
		}, 'uetest_shelters', testConnection)

	};

	models.Animal.index('name', { index: 'analyzed', analyzer: 'english', name: 'englishKeywords' });

	return models;
}

exports.createTestModels = createTestModels;

function insertTestData() {
	let models = createTestModels();
	return models.Shelter.insertMulti([
		//...
	])
		.then(() => {
			return models.Animal.insertMulti([
				//...
			]);
		})
		.then(() => models);
}

exports.insertTestData = insertTestData;
