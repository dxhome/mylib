'use strict';

const through = require('through');
const mylib = require('storj-lib');
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
    var limit = 0;
    var count = 0;
    var trimFront = 0;
    var trimBack = 0;
    var trimFrontSet = false;
    var trimBackSet = false;

    frame.shards.forEach(function(shard) {
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
 * Returns a through stream that trims the output based on the given range
 * @param {Number} trimFront - Number of bytes to trim off front of stream
 * @param {Number} totalBytes - The total length of the stream in bytes
 */
mylib.utils.createStreamTrimmer = function (trimFront, totalBytes) {
    let next = 0;
    let readBytes = 0;

    return through(function(data) {
        next += data.length;
        let offset = next - data.length;

        if (readBytes >= totalBytes) {
            return this.queue(null);
        }

        if (trimFront > next) {
            return this.queue(new Buffer([]));
        }

        let rangeStart = (offset > trimFront) ? 0 : (trimFront - offset);
        let rangeEnd = (next > (trimFront + totalBytes)) ?
            (trimFront + totalBytes) - offset : data.length;

        let trimmedSlice = data.slice(rangeStart, rangeEnd);
        readBytes += trimmedSlice.length;

        this.queue(trimmedSlice);
    });
};

mylib.constants.NET_REENTRY = 30000;

module.exports = mylib;
