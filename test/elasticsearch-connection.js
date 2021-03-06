// Copyright 2016 Zipscene, LLC
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

const http = require('http');
const { expect } = require('chai');

const ElasticsearchConnection = require('../lib/elasticsearch-connection');
const testUtils = require('./lib/test-utils');

const proxyPort = 9201;

describe('ElasticsearchConnection', function() {

	let conn;
	before(function() {
		this.timeout(0);
		return testUtils.resetAndConnect()
			.then((conn_) => {
				conn = conn_;
			});
	});

	describe('request', function() {

		it('should resolve with a response body', function() {
			return conn.request({
				method: 'GET',
				path: '/'
			}).then((body) => {
				body = JSON.parse(body);
				expect(body.status).to.equal(200);
			});
		});

		it('should not timeout w/ a slow connection', function() {
			this.timeout(0);

			let { indexConfigs } = testUtils.getConfig();
			let hostConfig = {
				host: `http://localhost:${ proxyPort }`,
				requestTimeout: 1000
			};
			let slowConnection = new ElasticsearchConnection(hostConfig, indexConfigs);

			let server = http.createServer((req, res) => {
				return setTimeout(() => {
					res.write(JSON.stringify({ status: 200 }));
					res.end();
				}, 950);
			});

			let serverPromise = new Promise((resolve, reject) => {
				server.listen(proxyPort, (err) => (err ? reject(err) : resolve()));
			});

			return Promise.all([ serverPromise, slowConnection.connectionWaiter.promise ])
				.then(() => slowConnection.request({ method: 'GET', path: '/' }))
				.then((body) => {
					body = JSON.parse(body);
					expect(body.status).to.equal(200);
				});
		});

	});

	describe('requestStream', function() {

		it('should resolve with a response stream', function() {
			this.timeout(1000000);
			return conn.requestStream({
				method: 'GET',
				path: '/'
			}).then((stream) => stream.intoString())
				.then((body) => {
					body = JSON.parse(body);
					expect(body.status).to.equal(200);
				});
		});

	});

});
