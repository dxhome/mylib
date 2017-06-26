'use strict';

const expect = require('chai').expect;
const mylib = require('.');
const JSLogger = require('my-jslogger');
const async = require('async');
const Readable = require('stream').Readable;

let logger = new JSLogger();
logger.pipe(process.stdout);

let server = process.env.SERVER || 'http://127.0.0.1:6382';
let user = process.env.USER || 'a@a.com';
let password = process.env.PASSWORD || 'password';
let bucketName = 'myBucket';

describe('Mylib Test - ', function () {
    let client;
    let bucket;

    before((done) => {
        let options = {
            baseURI: server,
            logger: logger,
            requestTimeout: 3000,
            basicAuth: {
                email: user,
                password: password},
        };
        client = mylib.BridgeClient(server, options);

        // create a new bucket
        client.createBucket({name: bucketName}, (err, data) => {
            if (err) {
                return done(err);
            }

            bucket = data;
            done();
        });
    });

    after((done) => {
       // clean up bucket
        client.destroyBucketById(bucket.id, done);
    });

    describe('multiple uploads ', function () {
        this.timeout(60000);

        it('should create a new upload', done => {
            let uploadData = {
                bucket: bucket.id,
                filename: 'myfile.exe',
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
            let parts = [
                {partNum: 7, size: 10, content: Buffer.alloc(10, 7)},
                {partNum: 2, size: 10, content: Buffer.alloc(10, 2)},
                {partNum: 5, size: 10, content: Buffer.alloc(10, 5)},
                {partNum: 3, size: 10, content: Buffer.alloc(10, 3)},
                {partNum: 1, size: 10, content: Buffer.alloc(10, 1)},
                {partNum: 6, size: 10, content: Buffer.alloc(10, 6)},
                {partNum: 8, size: 10, content: Buffer.alloc(10, 8)},
                {partNum: 4, size: 10, content: Buffer.alloc(10, 4)},
            ];

            let uploadData = {
                bucket: bucket.id,
                filename: 'test',
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

                        part.hmac = entry.hmac;
                    });
                }, (err) => {
                    if (err) {
                        return done(err);
                    }

                    // complete upload
                    client.completeUploadById(upload.id, parts.slice(0, 5), (err, file) => {
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

    });

});