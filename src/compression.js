const zlib = require('zlib')
const util = require('util')

const gzip = util.promisify(zlib.gzip)
const gunzip = util.promisify(zlib.gunzip)

module.exports = {gzip, gunzip}
