const _ = require('lodash');
const HttpConnector = require('elasticsearch/src/lib/connectors/http');
const zlib = require('zlib');
const zstreams = require('zstreams');


/**
 * Extends the Elasticsearch HttpConnector with ability to run streamed requests, using the same http
 * agent and request building/handling.
 *
 * @class ElasticsearchHttpConnector
 * @constructor
 * @param {Host} host - The host object representing the elasticsearch node we will be talking to.
 * @param {Object} [config] - Configuration options (extends the configuration options for ConnectionAbstract).
 *   @param {Number} [config.concurrency=10] - the maximum number of sockets that will be opened to this node.
 */
class ElasticsearchHttpConnector extends HttpConnector {

	constructor(host, config) {
		super(host, config);
	}

	/**
	 * Adapted from HttpConnector.request, but returns the request stream itself, instead of the response.
	 *
	 * @param {Object} params - ElasticSearch request parameters. See the following URL for more info:
	 *   https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/api-conventions.html
	 * @param {Function} cb
	 *   @param {Error} cb.err
	 *   @param {ReadableStream} cb.stream
	 */
	requestStream(params, cb) {
		// Build request parameters
		let reqParams = this.makeReqParams(params);

		// general clean-up procedure to run after the request
		// completes, has an error, or is aborted.
		let cleanUp = _.once((err, incoming) => {
			if ((err instanceof Error) === false) {
				err = undefined;
			}

			this.log.trace(params.method, reqParams, params.body, '[streamed response]', incoming.status);
			if (err) {
				cb(err);
			} else {
				cb(err, incoming);
			}
		});

		let request = this.hand.request(reqParams, (_incoming) => {
			let incoming = zstreams(_incoming);

			// Automatically handle unzipping incoming stream
			let encoding = (_incoming.headers['content-encoding'] || '').toLowerCase();
			if (encoding === 'gzip' || encoding === 'deflate') {
				incoming = incoming.pipe(zlib.createUnzip());
			}

			// Wrap IncomingMessage functinoality into the zstream
			_incoming.on('close', () => incoming.emit('close'));
			incoming.httpVersion = _incoming.httpVersion;
			incoming.headers = _incoming.headers;
			incoming.rawHeaders = _incoming.rawHeaders;
			incoming.trailers = _incoming.trailers;
			incoming.rawTrailers = _incoming.trailers;
			incoming.setTimeout = _incoming.setTimeout.bind(_incoming);
			incoming.method = _incoming.method;
			incoming.url = _incoming.url;
			incoming.statusCode = _incoming.statusCode;
			incoming.statusMessage = _incoming.statusMessage;
			incoming.socket = _incoming.socket;

			cleanUp(undefined, incoming);
		});

		request.on('error', cleanUp);

		request.setNoDelay(true);
		request.setSocketKeepAlive(true);

		if (params.body) {
			request.setHeader('Content-Length', Buffer.byteLength(params.body, 'utf8'));
			request.end(params.body);
		} else {
			request.end();
		}

		return () => {
			request.abort();
		};
	}

}

module.exports = exports = ElasticsearchHttpConnector;
