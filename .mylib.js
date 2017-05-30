'use strict';

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

// update file info
mylib.BridgeClient.prototype.updateFileInfo = function (bucketid, fileid, hmac, callback) {
    return this._request('PATCH', `/buckets/${bucketid}/files/${fileid}/info`, {
        hmac: hmac
    }, callback);
};

mylib.constants.NET_REENTRY = 30000;

module.exports = mylib;
