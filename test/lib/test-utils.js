const ElasticsearchConnection = require('../../lib').ElasticsearchConnection;
const ElasticsearchModel = require('../../lib').ElasticsearchModel;

const testConfig = {
	host: 'http://localhost:9200',
	indexConfigs: {
		'uetest_*': {
			shards: 16
		}
	},
	allTestIndexes: 'uetest_*'
};

let testConnection;

function getConnection() {
	return testConnection;
}

function connect() {
	if (testConnection) {
		testConnection.close();
	}
	testConnection = new ElasticsearchConnection(testConfig.host, testConfig.indexConfigs);
	return testConnection;
}

function resetData() {
	if (!testConnection) {
		return Promise.resolve();
	}
	return testConnection.request(`/${testConfig.allTestIndexes}/`, {
		method: 'DELETE'
	})
		.then(() => new Promise((resolve) => {
			// ElasticSearch doesn't necessarily delete the data right away, so we need to give it
			// some time to run the delete.
			setTimeout(resolve, 500);
		}));
}

function resetAndConnect() {
	return resetData()
		.then(connect);
}

exports.resetAndConnect = resetAndConnect;


function createTestModels() {
	// Need to make these more complete
	let models = {

		Animal: new ElasticsearchModel('Animal', {
			animalId: { type: String, index: true },
			name: { type: String, index: true },
			sex: { type: String, enum: [ 'male', 'female', 'unknown' ] },
			description: { type: String, index: true }
		}, 'uetest_animals', { parentType: 'Shelter' }),

		Shelter: new ElasticsearchModel('Shelter', {
			shelterId: { type: String, index: true },
			name: { type: String }
		}, 'uetest_shelters')

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
