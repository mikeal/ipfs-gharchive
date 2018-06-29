const farmhash = require('farmhash')

module.exports = str => {
  let hash = farmhash.hash32(str)
  return [Math.floor(hash / 100000000), Math.floor(hash / 1000000)].join('/')
}
