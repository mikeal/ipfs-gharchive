const zlib = require('zlib')
const JSONStream = require('json-stream')
const bent = require('bent')
const database = require('./database')

const get = bent()

const _opts = { format: 'dag-cbor', hashAlg: 'sha2-256' }

module.exports = async (filename, bulk) => {
  let stream = await get(`http://data.gharchive.org/${filename}`)
  let reader = stream.pipe(zlib.createUnzip()).pipe(JSONStream())
  let events = 0
  for await (let event of reader) {
    await bulk.write(event)
    events++
  }
  let cid = await bulk.db.ipfs.dag.put({events}, _opts)
  console.log({events})
  bulk.rawWrite(['receipts', filename], cid)
  return bulk.flush()
}

/* test */
const ipfsAPI = require('ipfs-api')
const ipfs = ipfsAPI('/ip4/127.0.0.1/tcp/5001')

let b = (async () => {
  let empty = await ipfs.dag.put({}, _opts)
  return require('./bulk')(database(ipfs, empty))
})()


const run = async (dt) => {
  let _dt = (new Date(dt))
  let [date, time] = _dt.toISOString().split('T')
  let [year, month, day] = date.split('-')
  let [hour] = time.split(':')
  if (hour[0] === '0') hour = hour.slice(1)
  let f = `${year}-${month}-${day}-${hour}.json.gz`
  let start = Date.now()
  console.log('start', f)
  let cid = await module.exports(f, await b)
  let end = Date.now()
  console.log('end', f, (end - start) / 1000, 'seconds.')
  console.log('cid', cid.toBaseEncodedString())
  dt += (1000 * 60 * 60)
  run(dt)
}
run(1514764800000)
