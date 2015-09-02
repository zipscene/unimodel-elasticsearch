const expect = require('chai').expect;

const testUtils = require('./lib/test-utils');
const { ElasticsearchModel } = require('../lib');


describe('ElasticsearchModel', function() {

	beforeEach(testUtils.resetAndConnect);

	it('should create models and initialize them through the index', function() {
		this.timeout(10000);
		let Animal = new ElasticsearchModel('Animal', {
			animalId: { type: String, index: true },
			name: { type: String, index: true },
			sex: { type: String, enum: [ 'male', 'female', 'unknown' ] },
			description: { type: String, index: true },
			found: { type: Date, index: true }
		}, 'uetest_animals', testUtils.getConnection(), { initialize: false });
		console.dir(Animal);
		return Animal.initialize().then(() => console.dir(Animal))
			.then(() => {
				let dog = Animal.create({
					animalId: 'dog',
					name: 'Charles Barkley',
					sex: 'male',
					description: 'an asshole.',
					found: new Date()
				});
				console.dir(dog);
				return dog.save();
			});
	});

});
