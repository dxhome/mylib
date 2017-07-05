'use strict';

const {expect, assert} = require('chai');
const mylib = require('.');
const JSLogger = require('my-jslogger');
const async = require('async');
const Readable = require('stream').Readable;
const randomstring = require("randomstring");
const crypto = require('crypto');
let logger = new JSLogger();
logger.pipe(process.stdout);

let server = process.env.SERVER || 'http://127.0.0.1:6382';
let user = process.env.USER || 'a@a.com';
let password = process.env.PASSWORD || 'password';
let bucketName = 'myBucket';

describe('Mylib Test - ', function () {
    this.timeout(600000);

    let client;
    let bucket;
    let filename = randomstring.generate({
        length: 12,
        charset: 'alphabetic'
    });

    function _getBucketId (user, bucketName) {
        return mylib.utils.calculateBucketId(user, bucketName);
    }


    before((done) => {
        let options = {
            baseURI: server,
            logger: logger,
            requestTimeout: 30000,
            basicAuth: {
                email: user,
                password: password},
        };
        client = mylib.BridgeClient(server, options);

        let bucketid = _getBucketId(user, bucketName);

        client.getBucketById(bucketid, (err, data) => {
            if (err) {
                // create a new bucket
                client.createBucket({name: bucketName}, (err, data) => {
                    if (err) {
                        return done(err);
                    }

                    bucket = data;
                    return done();
                });
            } else {
                bucket = data;
                return done();
            }

        });

    });

    // after((done) => {
    //    // clean up bucket
    //    client.destroyBucketById(bucket.id, done);
    // });

    describe('store file 2', function () {
        it('should store normal file successfully', done => {
            let fileSize = 20;
            let buf = Buffer.alloc(fileSize, 'a');
            let stream = new Readable({
                read(size) {
                    this.push(buf);
                    this.push(null);
                }
            });

            client.storeFileInBucket2(bucket.id, stream, {fileName: filename, fileSize: fileSize}, (err, file) => {
                if (err) {
                    return done(err);
                }

                console.log(file);

                done();
            })
        });

        it('should store 0 size file successfully from a file', done => {

            client.storeFileInBucket2(bucket.id, null, {fileName: filename, fileSize: 0}, (err, file) => {
                if (err) {
                    return done(err);
                }

                console.log(file);

                done();
            })
        });

    });

    describe('list files 2', function () {
        let names = ['a', 'aa', 'b', 'bb', 'c', 'cc', 'A', 'AA', 'B', 'BB', 'C', 'CC'];
        let files = [];
        before((done) => {
            // create some files
            async.each(names, (name, next) => {
                client.storeEmptyFileInBucket(bucket.id, null, {fileName: name}, (err, file) => {
                    if (err) {
                        return next(err);
                    }

                    files.push(file);
                    next();
                });
            }, done);
        });

        after((done) => {
            async.each(files, (file, next) => {
                client.removeFileFromBucket(bucket.id, file.id, next);
            }, done);
        });

        it('should list files successfully without query', done => {
            client.listFilesInBucket2(bucket.id, {startAfter: null}, (err, results) => {
                if (err) {
                    return done(err);
                }

                console.log('found files: ', results.map(x => x.filename));

                assert(results.length >= files.length);

                done();
            })
        });

        it('should list files successfully with query startAfter=b', done => {
            client.listFilesInBucket2(bucket.id, {startAfter: 'b'}, (err, results) => {
                if (err) {
                    return done(err);
                }

                console.log('found files: ', results.map(x => x.filename));

                assert(results.length >= 3);

                done();
            })
        });
    });

    describe('multiple uploads ', function () {
        it('should list all uploads', done => {
            let query = null;

            client.listUploads(bucket.id, query, (err, results) => {
                if (err) {
                    return done(err);
                }

                console.log(results);
                done();
            });
        });

        it('should list uploads with keyMarker only', done => {
            let query = {keyMarker: 'myfile.ex'};

            client.listUploads(bucket.id, query, (err, results) => {
                if (err) {
                    return done(err);
                }

                console.log(results);
                done();
            });
        });

        it('should list uploads with both keyMarker and uploadidMarker', done => {
            let query = {
                keyMarker: 'myfile.exe',
                uploadidMarker: '595122151acdbdeb688f4260',
            };

            client.listUploads(bucket.id, query, (err, results) => {
                if (err) {
                    return done(err);
                }

                console.log(results);
                done();
            });
        });

        it('should create a new upload', done => {
            let uploadData = {
                bucket: bucket.id,
                filename: filename,
                mimetype: 'application/x-msdownload'
            };
            client.createUpload(uploadData, (err, upload) => {
                if (err) {
                    return done(err);
                }

                console.log(upload);

                client.destroyUploadById(upload.id, (err) => {
                    if (err) {
                        return done(err);
                    }

                    client.getUploadById(upload.id, (err, upload2) => {
                        if (err) {
                            return done(err);
                        }

                        console.log(upload2);

                        done();
                    });

                });

            });
        });

        it('should create a new upload, add several parts and complete it', done => {
            let len = 3;
            let parts = [
                {partNum: 7, size: len, content: Buffer.alloc(len, 7)},
                {partNum: 2, size: len, content: Buffer.alloc(len, 2)},
                {partNum: 5, size: len, content: Buffer.alloc(len, 5)},
                {partNum: 3, size: len, content: Buffer.alloc(len, 3)},
                {partNum: 1, size: len, content: Buffer.alloc(len, 1)},
                {partNum: 6, size: len, content: Buffer.alloc(len, 6)},
                {partNum: 8, size: len, content: Buffer.alloc(len, 8)},
                {partNum: 4, size: len, content: Buffer.alloc(len, 4)},
            ];

            let uploadData = {
                bucket: bucket.id,
                filename: filename,
                mimetype: 'application/octet-stream'
            };
            client.createUpload(uploadData, (err, upload) => {
                if (err) {
                    return done(err);
                }

                async.eachLimit(parts, 6, (part, next) => {
                    let stream = new Readable({
                        read(size) {
                            this.push(part.content);
                            this.push(null);
                        }
                    });

                    client.addUploadPart(upload.id, part.partNum, part.size, stream, (err, entry) => {
                        if (err) {
                            return next(err);
                        }

                        console.log(entry);

                        part.hmac = entry.hmac;
                        next();
                    });
                }, (err) => {
                    if (err) {
                        return done(err);
                    }

                    // complete upload
                    let completeParts = parts.slice(0, 5).map((part) => {
                        let hasher = crypto.createHash('md5');
                        hasher.update(part.content);
                        return {
                            partNum: part.partNum,
                            eTag: hasher.digest('hex'),
                        };
                    });
                    client.completeUploadById(upload.id, completeParts, (err, file) => {
                        if (err) {
                            return done(err);
                        }

                        console.log(file);

                        // read file content
                        client.createFileStream(bucket.id, file.id, (err, filedata) => {
                            if (err) {
                                return done(err);
                            }

                            let temp = [];
                            filedata.on('error', done);
                            filedata.on('data', function(buffer) {
                                temp.push(buffer);
                            });
                            filedata.on('end', function() {
                                let buff = Buffer.concat(temp);

                                console.log(buff);
                                done();
                            });

                        });
                    });

                });

            });
        });

        it('should complete an upload successfully', done => {
            let len = 6;
            let parts = [
                {partNum: 7, size: len, content: Buffer.alloc(len, 7)},
                {partNum: 2, size: len, content: Buffer.alloc(len, 2)},
                {partNum: 5, size: len, content: Buffer.alloc(len, 5)},
                {partNum: 3, size: len, content: Buffer.alloc(len, 3)},
                {partNum: 1, size: len, content: Buffer.alloc(len, 1)},
                {partNum: 6, size: len, content: Buffer.alloc(len, 6)},
                {partNum: 8, size: len, content: Buffer.alloc(len, 8)},
                {partNum: 4, size: len, content: Buffer.alloc(len, 4)},
            ];
            let id = '5952366b9b5edd2940337572';

            // complete upload
            client.completeUploadById(id, parts.slice(0, 5), (err, file) => {
                if (err) {
                    return done(err);
                }

                console.log(file);

                // read file content
                client.createFileStream(bucket.id, file.id, (err, filedata) => {
                    if (err) {
                        return done(err);
                    }

                    let temp = [];
                    filedata.on('error', done);
                    filedata.on('data', function(buffer) {
                        temp.push(buffer);
                    });
                    filedata.on('end', function() {
                        let buff = Buffer.concat(temp);

                        console.log(buff);
                        done();
                    });

                });
            });
        });

    });

});