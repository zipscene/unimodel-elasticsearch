// const { expect } = require('chai');

const testUtils = require('./lib/test-utils');
const { ElasticsearchModel } = require('../lib');


describe('ElasticsearchModel', function() {

	beforeEach(testUtils.resetAndConnect);

	it('should create models and initialize them through the index', function() {
		this.timeout(10000);
		let Animal = new ElasticsearchModel('Animal', {
			animalId: { type: String, index: true, id: true },
			name: { type: String, index: true },
			sex: { type: String, enum: [ 'male', 'female', 'unknown' ] },
			description: { type: String, index: true },
			found: { type: Date, index: true }
		}, 'uetest_animals', testUtils.getConnection(), { initialize: false });
		console.dir(Animal);
		return Animal.initialize().then(() => console.dir(Animal))
			.then(() => {
				let dog = Animal.create({
					animalId: 'dog-charles-barkley-male',
					name: 'Charles Barkley',
					sex: 'male',
					description: 'this dog does not exist',
					found: new Date()
				});
				console.dir(dog);
				return dog.save();
			})
			.then((dog) => {
				const dataDog = dog.getData();
				dataDog.name = 'Ein';
				dataDog.description = 'what the hell is a data dog?';
				return dog.save();
			});
	});

});
