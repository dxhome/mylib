'use strict';

const through = require('through');
const mime = require('mime');
const async = require('async');
const fs = require('fs');
const path = require('path');
const merge = require('merge');
const crypto = require('crypto');
const mylib = require('storj-lib');
const Transform = require('readable-stream').Transform;

const consts = require('./.consts');

// const url = require('url');

// mylib.utils.getContactURL = function(contact) {
//     return [
//         consts.MYLIB_NAME , '://', contact.address, ':', contact.port, '/', contact.nodeID
//     ].join('');
// };
// mylib.Contact.isValidUrl = function(uri) {
//     let contact = null;
//
//     try {
//         contact = url.parse(uri);
//         assert(contact.protocol === consts.MYLIB_NAME + ':', 'Invalid protocol');
//         assert(
//             Buffer.from(contact.path.substr(1), 'hex').length * 8 === 160,
//             'Invalid node ID'
//         );
//     } catch (err) {
//         return false;
//     }
//
//     return true;
// };

/**
 * update file hmac info
 * @param {String} bucketid - build id
 * @param {String} fileid - file id
 * @param {Object} hmac - hmac info
 * @param {String} hmac.type - hmac type, md5, sha256, ...
 * @param {String} hmac.value - digest value
 * @param callback
 * @returns
 */
mylib.BridgeClient.prototype.updateFileInfo = function (bucketid, fileid, hmac, callback) {
    return this._request('PATCH', `/buckets/${bucketid}/files/${fileid}/info`, {
        hmac: hmac
    }, callback);
};


/**
 * Returns the skip/limit params for downloading a file slice
 * @private
 * @param {Object} frame - The frame object from the bridge
 * @param {Number} bytesStart - The starting byte for slice
 * @param {Number} bytesEnd - The ending byte for slice
 */
mylib.BridgeClient.prototype._getSliceParams = function(frame, bytesStart, bytesEnd) {
    var skip = 0;
    var limit = 1;
    var count = 0;
    var trimFront = 0;
    var trimBack = 0;
    var trimFrontSet = false;
    var trimBackSet = false;

    frame.shards.forEach(function(shard) {
        if (typeof shard !== 'object') {
            throw new Error('shard in frame is not an object; maybe server code is not up-to-date');
        }

        count += shard.size;

        if (bytesStart > count) {
            skip++;
        } else if (!trimFrontSet) {
            trimFront = bytesStart - ( count - shard.size );
            trimFrontSet = true;
        }

        if (bytesEnd > count) {
            limit++;
        } else if (!trimBackSet){
            trimBack = count - bytesEnd;
            trimBackSet = true;
        }
    });

    return {
        skip: skip,
        limit: limit,
        trimFront: trimFront,
        trimBack: trimBack
    };
};

/**
 * create empty file entry on server side
 * @param id - bucket id
 * @param token - PUSH token (no use)
 * @param opts - options
 * @param cb
 * @returns {*}
 */
mylib.BridgeClient.prototype.storeEmptyFileInBucket = function(id, token, opts, cb) {
    let self = this;
    let retry = 0;
    let fileName = opts.fileName;

    function _createFileStagingFrame(next) {
        self._logger.info('Creating empty file staging frame');
        self.createFileStagingFrame(function(err, frame) {
            if (err) {
                self._logger.error(err.message);
                return next(err);
            }

            next(null, frame);
        });
    }

    function _completeFileEntry(frame, next) {
        self._logger.info('Creating empty file entry.. (retry: %s)', retry);
        self._request('POST', '/buckets/' + id + '/files', {
            frame: frame.id,
            mimetype: mime.lookup(fileName),
            filename: fileName
        }, function(err, fileobj) {
            if (err) {
                if (retry < 6) {
                    retry++;
                    return _completeFileEntry(frame, next);
                }

                self._logger.error(err.message);
                return next(err);
            }

            next(null, fileobj);
        });
    }

    async.waterfall([
        _createFileStagingFrame,
        _completeFileEntry,
    ], function (err, fileobj) {
        if (err) {
            return cb(err);
        }

        cb(null, fileobj);
    })

};

/**
 * Stores a file in the bucket and update hmac with md5sum; support empty file creation
 * @param {String} bucketid - Unique bucket ID
 * @param {String} file - Path to file to store
 * @param {Function} callback
 */
// eslint-disable-next-line max-params
mylib.BridgeClient.prototype.storeFileInBucket2 = function(bucketid, file, opts, cb) {
    const self = this;
    let retry = 6;
    let hasher = crypto.createHash('md5');
    let inputStream;
    let fileName;
    let fileOrgName; // mainly used for uploads
    let fileSize = 0;

    if (typeof opts === 'function') {
        cb = opts;
        opts = {};
    }

    if (typeof file === 'string') {
        fileName = path.basename(file).split('.crypt')[0];
        fileOrgName = fileName;
        fileSize = fs.statSync(file).size;
        inputStream = fs.createReadStream(file);
    } else {
        inputStream = file;
        fileName = opts.fileName;
        fileOrgName = opts.fileOrgName ? opts.fileOrgName : fileName;
        fileSize = opts.fileSize;
    }
    let notEncrypt = (typeof opts.notEncrypt !== 'undefined') ? opts.notEncrypt : false;

    let fileid = mylib.utils.calculateFileId(bucketid, fileName);

    if (fileName === null) {
        return next(new Error('cannot support writing without filename'));
    }

    if (fileSize === null) {
        return next(new Error('cannot support writing without size'));
    }

    function _genCrypterSecret (seed, encryptionKey) {
        if (encryptionKey) {
            let fileKey = mylib.DeterministicKeyIv.getDeterministicKey(
                encryptionKey, seed);
            return new mylib.DeterministicKeyIv(fileKey, seed);
        } else {
            return null;
        }
    }

    function checkFileExist(next) {
        self.getFileInfo(bucketid, fileid, function(err, file) {
            if (file) {
                return next(new Error(fileName + ' exists in bucket.'));
            }

            next();
        });
    }

    function createToken(next) {
        self.createToken(bucketid, 'PUSH', function(err, token) {
            if (err) {
                if (retry < 6) {
                    retry++;
                    return createToken(next);
                }

                return next(err);
            }
            next(null, token);
        });
    }

    function createSourceStream(token, next) {
        let hasherStream = new Transform({
            transform(chunk, encoding, callback) {
                hasher.update(chunk, encoding);
                callback(null, chunk, encoding);
            }
        });

        if (fileSize > 0) {
            let encrypter = null;
            let secret;
            if (notEncrypt) {
                secret = null;
            } else {
                let seed = mylib.utils.calculateFileId(bucketid, fileOrgName);
                secret = _genCrypterSecret(seed, token.encryptionKey);
            }

            if (secret) {
                // encrypted data
                encrypter = new mylib.EncryptStream(secret);
                // return stream as early as possible
                next(null, token, inputStream.pipe(hasherStream).pipe(encrypter));
            } else {
                // no encryption data
                next(null, token, inputStream.pipe(hasherStream));
            }

            inputStream.on('error', (err) => {
                hasher.emit('error', err).end();
                hasherStream.emit('error', err).end();
                if (encrypter) encrypter.emit('error', err).end();
            });
        } else {
            // for empty file creating, no
            next(null, token, null);
        }
    }


    function storeInBucket(token, srcStream, next) {
        function _updateCheckSum(bucketid, fileid, hmac) {
            self.updateFileInfo(bucketid, fileid, hmac, (err) => {
                if (err) {
                    return self._logger.warn('cannot update md5sum for file %s on bucket %s, %s', fileName, bucketid, err.message);
                } else {
                    return self._logger.info('updated md5sum for file %s on bucket %s, md5sum: %s', fileName, bucketid, hmac.value);
                }
            });
        }

        if (fileSize === 0) {
            self.storeEmptyFileInBucket(
                bucketid,
                token,
                {
                    fileName: fileName,
                },
                function(err, file) {
                    if (err) {
                        return next(err);
                    }

                    file.size = 0;
                    file.hmac = {
                        type: 'md5',
                        value: 'd41d8cd98f00b204e9800998ecf8427e'
                    };
                    next(null, file);

                    _updateCheckSum(bucketid, file.id, file.hmac);
                }
            );
        } else {
            self.storeFileInBucket(
                bucketid,
                token,
                srcStream,
                {
                    fileName: fileName,
                    fileSize: fileSize,
                },
                function(err, file) {
                    if (err) {
                        return next(err);
                    }

                    let checksum = hasher.digest('hex');
                    file.hmac = {
                        type: 'md5',
                        value: checksum
                    };
                    next(null, file);

                    _updateCheckSum(bucketid, file.id, file.hmac);
                }
            );
        }

    }

    async.waterfall([
        checkFileExist,
        createToken,
        createSourceStream,
        storeInBucket
    ], function (err, file) {
        if (err) {
            return cb(err);
        }

        cb(null, file);
    });
};

/**
 * Create a stream for a given slice of a file; return 'encryptionKey' from token
 * @param {Object} options
 * @param {String} options.bucket - The bucket ID
 * @param {String} options.file - The file ID
 * @param {Number} options.start - The byte position to start slice
 * @param {Number} options.end - The byte position to end slice
 * @param {Array} options.exclude - exclude farmers for reading
 */
mylib.BridgeClient.prototype.createFileSliceStream2 = function(options, callback) {
    let self = this;

    self._blacklist.toObject((err, blacklist) => {
        let excludes = [];
        if (blacklist) {
            excludes = Array.isArray(options.exclude) ? blacklist.concat(options.exclude) : blacklist;
        } else {
            excludes = options.exclude;
        }
        self._logger.debug('creating file slice stream by excluding %j', excludes);

        self.getFrameFromFile(options.bucket, options.file, function(err, frame) {
            if (err) {
                return callback(err);
            }

            let sliceOpts = self._getSliceParams(frame, options.start, options.end);

            self.createToken(options.bucket, 'PULL', function(err, token) {
                if (err) {
                    return callback(err);
                }

                self.getFilePointers({
                    bucket: options.bucket,
                    token: token.token,
                    file: options.file,
                    skip: sliceOpts.skip,
                    limit: sliceOpts.limit,
                    exclude: excludes
                }, function(err, pointers) {
                    if (err) {
                        return callback(err);
                    }

                    self._logger.info('Retrieving data from %d pointer(s).. ', pointers.length);

                    self.resolveFileFromPointers(pointers, function(err, stream) {
                        if (err) {
                            return callback(err);
                        }

                        // let trimStream = stream.pipe(mylib.utils.createStreamTrimmer(
                        //     sliceOpts.trimFront,
                        //     options.end - options.start + 1
                        // ));
                        // trimStream.encryptionKey = token.encryptionKey;
                        //
                        // callback(null, stream.pipe(trimStream));

                        stream.encryptionKey = token.encryptionKey;
                        callback(null, stream);
                    });
                });
            });
        });
    });

};

/**
 * Create a readable stream from the given bucket and file id, use client backlist by default
 * @param {String} bucket - The unique bucket ID
 * @param {String} file - The unique file ID
 * @param {Object} [opt]
 * @param {Array} [opt.exclude] - Exclude these nodeID's from pointers
 * @param {Function} cb - Receives (err, stream)
 */
mylib.BridgeClient.prototype.createFileStream2 = function(bucket, file, opt, cb) {
    let self = this;

    self._blacklist.toObject((err, blacklist) => {
        if (blacklist) {
            opt.exclude = Array.isArray(opt.exclude) ? blacklist.concat(opt.exclude) : blacklist;
        }
        self._logger.debug('creating file stream by excluding %j', opt.exclude);

        self.createFileStream(bucket, file, opt, cb);
    });
};

/**
 * Returns a through stream that trims the output based on the given range
 * @param {Number} trimFront - Number of bytes to trim off front of stream
 * @param {Number} totalBytes - The total length of the stream in bytes
 */
mylib.utils.createStreamTrimmer = function (trimFront, totalBytes) {
    return through(function(data) {
        if (this.name === undefined) {
            this.name = String(Math.random()*1000).slice(0,5);
        }
        if (this.next === undefined) {
            this.next = 0;
        }
        if (this.readBytes === undefined) {
            this.readBytes = 0;
        }

        this.next += data.length;
        let offset = this.next - data.length;

        if (this.readBytes >= totalBytes) {
            return this.queue(null);
        }

        if (trimFront > this.next) {
            return this.queue(new Buffer([]));
        }

        // console.log('%s: %d %d %d %d %d %d', this.name, data.length, offset, this.next, this.readBytes, trimFront, totalBytes);

        let rangeStart = (offset > trimFront) ? 0 : (trimFront - offset);
        let rangeEnd = (this.next > (trimFront + totalBytes)) ?
            (trimFront + totalBytes) - offset : data.length;

        // console.log('%s: trimmedSlice %d - %d', this.name, rangeStart, rangeEnd+1);
        let trimmedSlice = data.slice(rangeStart, rangeEnd+1);
        this.readBytes += trimmedSlice.length;

        this.queue(trimmedSlice);
    });
};

mylib.constants.NET_REENTRY = 30000;

/**
 * Lists the uploads for a bucket
 * @param {String} bucketid - Unique bucket ID
 * @param {Object} query - query info, {.keyMarker, .uploadidMarker}
 * @param {Function} callback
 */
mylib.BridgeClient.prototype.listUploads = function(bucketid, query, callback) {
    let body = merge({bucket: bucketid}, query);
    return this._request('GET', '/uploads', body, callback);
};

/**
 * create a new upload
 * @param upload - upload content {.bucket, .filename, .mimetype}
 * @param callback
 * @returns {{abort}}
 */
mylib.BridgeClient.prototype.createUpload = function (upload, callback) {
    return this._request('POST', '/uploads', upload, callback);
};

/**
 * get an upload
 * @param id - upload id
 * @param callback
 * @returns {{abort}}
 */
mylib.BridgeClient.prototype.getUploadById = function (id, callback) {
    return this._request('GET', '/uploads/'+id, {}, callback);
};

/**
 * abort an upload
 * @param id - upload id
 * @param callback
 * @returns {{abort}}
 */
mylib.BridgeClient.prototype.destroyUploadById = function (id, callback) {
    return this._request('DELETE', '/uploads/'+id, {}, callback);
};

/**
 * complete an upload
 * @param id - upload id
 * @param parts - parts to complete upload, [{.partNum, .eTag}, ...]
 * @param callback
 * @returns {{abort}}
 */
mylib.BridgeClient.prototype.completeUploadById = function (id, parts, callback) {
    return this._request('POST', '/uploads/'+id, {parts: parts}, callback);
};

/**
 * add a new part to upload
 * @param id - upload id
 * @param partNum - part number
 * @param opts - (.size, .notEncrypt)
 * @param content - upload part content, a file or readable stream
 * @param callback
 * @returns {{abort}}
 */
mylib.BridgeClient.prototype.addUploadPart = function (id, partNum, opts, content, callback) {
    let self = this;

    self.getUploadById(id, (err, upload) => {
        if (err) {
            return callback(err);
        }

        // store part as a normal file first and remove bucket entry right after success
        // use the left frame to create a part
        self.storeFileInBucket2(upload.bucket, content, {
            fileName: `part${partNum}-${upload.name}`,
            fileOrgName: upload.name,
            fileSize: opts.size,
            notEncrypt: opts.notEncrypt,
        }, (err, file) => {
            if (err) {
                return callback(err);
            }

            // remove file entry but keep frame
            self.removeFileFromBucket(upload.bucket, file.id, (err) => {
                if (err) {
                    return callback(err);
                }

                // add part
                let partBody = {
                    partnum: partNum,
                    frame: file.frame,
                    hmac: file.hmac,
                };
                return self._request('POST', '/uploads/' + id + '/parts', partBody, callback);
            });
        });

    });

};

/**
 * Lists the files stored in a bucket 2, support additional query
 * @param {String} id - Unique bucket ID
 * @param {Object} query - query info, {.startAfter}
 * @param {Function} callback
 */
mylib.BridgeClient.prototype.listFilesInBucket2 = function(id, query, callback) {
    return this._request('GET', '/buckets/' + id + '/files', query?query:{}, callback);
};



/**
 * Generate audits for shard and add to frame, support reuse existing shard and add to frame
 * @private
 * @param {UploadState} state - The shard upload state machine
 * @param {Object} meta - Shard metadata reference
 * @param {Function} done - To be called on task complete
 */
mylib.BridgeClient.prototype._handleShardTmpFileFinish = function(state, meta, done) {
    var self = this;
    var hash = mylib.utils.rmd160(meta.hash);
    var auditGenerator = new mylib.AuditStream(3);

    var rs = self._store.createReadStream(meta.tmpName);

    self._logger.info('Hash for this shard is: %s', hash);

    function _handleError(err) {
        self._logger.warn('Failed to upload shard...');
        state.cleanup(function(err2){
            // If there was an error, and we failed to cleanup temporary files, we
            // propogate the collection of files that failed back to the caller. The
            // logic is that the upload error isn't as immediately important as
            // loosing track of files in the user's blobstore (s3, filesystem, etc)
            return state.callback(err2 || err);
        });
    }

    function _teardownAuditListeners() {
        auditGenerator.removeAllListeners();
    }

    rs.on('error', _handleError);
    state.on('killed', _teardownAuditListeners);

    function _getContract(blacklist) {
        if (state.killed) {
            return done();
        }

        if (!meta.challenges && !meta.tree) {
            meta.challenges = auditGenerator.getPrivateRecord().challenges;
            meta.tree = auditGenerator.getPublicRecord();
            self._logger.info('Audit generation for shard done.');
        }

        self._logger.info('Waiting on a storage offer from the network...');

        var addShardToFrame = self.addShardToFileStagingFrame(meta.frame.id, {
            hash: hash,
            size: meta.size,
            index: meta.index,
            challenges: meta.challenges,
            tree: meta.tree,
            exclude: blacklist,
        }, function(err, pointer) {
            if (state.killed) {
                return done();
            }

            if (err) {
                return _handleError(err);
            }

            if (pointer.operation === 'REUSE') {
                // support new operation 'REUSE' to use existing stored shard
                // and bypass unnecessary data transferring
                self._logger.info('Contract reused and skip shard transferring..., reused with: %j, ', pointer.farmer);
                self._shardTransferComplete(state, meta.frame, done);
            } else {
                self._startTransfer(pointer, state, meta, done);
            }
        });

        // Only register listener if addShardToFrame succeeds
        if (addShardToFrame) {
            state.removeListener('killed', _teardownAuditListeners);
            state.on('killed', addShardToFrame.cancel);
        }
    }

    self._blacklist.toObject(function(e, blacklist) {
        if (meta.challenges && meta.tree) {
            _getContract(blacklist);
        } else {
            rs.pipe(auditGenerator).on('finish', _getContract, blacklist);
        }
    });
};

module.exports = mylib;
