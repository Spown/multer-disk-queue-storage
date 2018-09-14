var fs = require('fs'),
	stream = require('stream'),
	os = require('os'),
	path = require('upath'),
	mkdirp = require('mkdirp'),
	mime = require('mime'),
	concat = require('concat-stream'),
	staticVars = require('static-vars'),
	_ = require('lodash'),
	QS_INIT = 0, QS_STARTED = 1, QS_PROCESSING = 2, QS_FINISHED = 7
;

function slash(str) {
	var isExtendedLengthPath = /^\\\\\?\\/.test(str||''),
		hasNonAscii = /[^\x00-\x80]+/.test(str||'')
	;
	if (isExtendedLengthPath || hasNonAscii) { return str; }
	return (str||'').replace(/\\/g, '/');
}

function processQueue(q) {
	var concurrentNum = 0,
		multerDiskQueueStorageMaxConcurrent = staticVars.get('multerDiskQueueStorageMaxConcurrent'),
		toDelete = []
	;
	_.each(q, function(qi, idx) {
		if (qi.state === QS_INIT && concurrentNum <= multerDiskQueueStorageMaxConcurrent) {
			qi.read();
			concurrentNum++;
		} else if (qi.state > QS_INIT && qi.state < QS_FINISHED) {
			concurrentNum++;
		} else if (qi.state === QS_FINISHED) {
			toDelete.push(idx);
		}
	});
	_.pullAt(q, toDelete);
	if (q.length) {
		staticVars.set('multerDiskQueueStorage', q);
	} else if (this._repeat && q.length === 0) {
		staticVars.del('multerDiskQueueStorage');
		clearInterval(this);
	}
}

function ensureQueueExists(qPollingInterval) {
	var q = [], si;
	if (!staticVars.has('multerDiskQueueStorage')) { //ensure queue
		staticVars.set('multerDiskQueueStorage', q);
		si = setInterval(processQueue, qPollingInterval, q);
	} else {
		q = staticVars.get('multerDiskQueueStorage');
	}
	return q;
}

function DiskStorage(opts) {
	if (!_.isNumber(opts.maxConcurrent) || opts.maxConcurrent <= 0 ) {
		opts.maxConcurrent = 8;
	}
	staticVars.set('multerDiskQueueStorageMaxConcurrent', opts.maxConcurrent);
    
	if (!_.isNumber(opts.qPollingInterval) || opts.qPollingInterval <= 0 ) {
		opts.qPollingInterval = 100;
	}
	
	if (_.isString(opts.filename)) {
		this.setFilename = function() {
			return opts.filename;
		};
	} else if (_.isFunction(opts.filename)) {
		this._needBuffer = true;
		this.setFilename = function(req, queueItem) {
			return opts.filename.apply(this, arguments);
		};
	} else if (!opts.filename) {
        this.setFilename = function (req, queueItem) {
            return Date.now()+'_'+queueItem.file.originalname.replace(/\.[^/.]+$/, "")+'.'+mime.extension(queueItem.file.mimetype);
        };
    }
	
	if (_.isString( opts.destination )) {
		mkdirp.sync(opts.destination);
		this.setDestination = function() {
			return opts.destination;
		};
	} else if (_.isFunction(opts.destination)) {
		this._needBuffer = true;
		this.setDestination = function (req, queueItem) {
			return opts.destination.apply(this, arguments);
		} ;
	}

}

function attachToFsWriteStram(qi, pw, cb) {
	if (!qi) {
		cb(new Error('no queue item given!'));
		return;
	} else if (_.isError(qi.destination)) {
		cb(qi.destination)
		return;
	} else if (_.isError(qi.filename)) {
		cb(qi.filename)
		return;
	} else if (!qi.path) {
		cb(new Error('no or wrong path given!'));
		return;
	}
	var writeStream = fs.createWriteStream( qi.path );
	writeStream.on('error', function (er, d) {
		qi.state = QS_FINISHED;
		cb(er, d);
	});
	writeStream.on('finish', function(er, d) {
		qi.state = QS_FINISHED;
		cb(null, {
			destination: qi.destination,
			filename: qi.filename,
			path: qi.path,
			size: writeStream.bytesWritten
		});
	});
	if (Buffer.isBuffer(pw)) {
		writeStream.write(pw, function() {
			writeStream.end();
		});
	} else if(pw instanceof stream.Stream && typeof (pw._read === 'function') && typeof (pw._readableState === 'object')) {
		pw.pipe( writeStream );
	}
}

function produceFullPath(qi) {
	return qi.path = ((qi.filename && qi.destination && _.isString(qi.filename) && _.isString(qi.destination)) ?
		path.normalize(path.join(qi.destination, qi.filename)) :
		undefined)
	;
}

DiskStorage.prototype._handleFile = function _handleFile(req, file, cb) {
	var ds = this,
		queueItem = {},
		ret = {},
		queueNamePfx = 0,
		queueName = Date.now(),
		multerDiskQueueStorage = ensureQueueExists(ds.qPollingInterval)
	;
	while ( _.some(multerDiskQueueStorage, {'queueName': '' + queueName + (++queueNamePfx)}) );
	queueItem.queueName = '' + queueName + queueNamePfx;
	queueItem.file = file;
	queueItem.state = QS_INIT;
	_.isString(ds.filename) && (queueItem.filename = ds.filename);
	_.isString(ds.destination) && (queueItem.destination = path.normalize(ds.destination));
	produceFullPath(queueItem);

	queueItem.read = function () {
		var _dest = ds.setDestination(req, queueItem);
		if (queueItem.state === QS_INIT) {
			if (ds._needBuffer) {
				file.stream.pipe(concat(function(buffer) {
					queueItem.state = QS_PROCESSING;
					queueItem.buffer = buffer;
					queueItem.destination = _.isString(_dest) ? path.normalize(_dest) : _dest;
					queueItem.filename = ds.setFilename(req, queueItem);
					produceFullPath(queueItem);
					attachToFsWriteStram(queueItem, buffer, cb);
				}));
			} else {
				attachToFsWriteStram(queueItem, file.stream, cb);
			}
			queueItem.state = QS_STARTED;
		}
	};
	multerDiskQueueStorage.push(queueItem);
	processQueue(multerDiskQueueStorage);
};

DiskStorage.prototype._removeFile = function _removeFile(req, file, cb) {
	var path = file.path;

	delete file.destination;
	delete file.filename;
	delete file.path;

	fs.unlink(path, cb);
};

module.exports = function (opts) {
	return new DiskStorage(opts);
};