const database = require('src/database')
const bulk = require('src/bulk')

exports.bulk = (ipfs, root) => {
  let db = database(ipfs, root)
  return bulk(db)
}
