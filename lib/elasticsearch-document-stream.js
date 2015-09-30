const inheritz = require('inheritz');
const pasync = require('pasync');
const { Transform } = require('zstreams');
const { DocumentStream } = require('zs-unimodel');

class ElasticsearchDocumentStream extends Transform {

	constructor(model, isPartialDocument = false) {
		super({ objectMode: true });
		this._model = model;
		this._isPartialDocument = isPartialDocument;
		this._totalWaiter = pasync.waiter();
	}

	setTotal(total) {
		this._totalWaiter.resolve(total);
	}

	_transform(obj, encoding, cb) {
		let newDoc;
		try {
			newDoc = this._model._createExisting(obj, this._isPartialDocument);
		} catch (ex) {
			return cb(ex);
		}
		this.push(newDoc);
		cb();
	}

	getTotal() {
		return this._totalWaiter.promise;
	}

}

inheritz(ElasticsearchDocumentStream, DocumentStream);

module.exports = ElasticsearchDocumentStream;
