const complex = require('ipld-complex-graph-builder')
const bent = require('bent')
const zlib = require('zlib')
const JSONStream = require('json-stream')
const identifyActor = require('./identifyActor')
const identifyRepo = require('./identifyRepo')
const cbor = require('ipld-complex-graph-builder/fancy-cbor')(() => {})
const multihashes = require('multihashes')
const CID = require('cids')
const Block = require('ipfs-block')
const crypto = require('crypto')
const shard = require('./shard')
const util = require('util')
const fs = require('fs')
const tmp = require('tmp')

const get = bent()

const fourHours = 1000 * 60 * 60 * 4

const sha2 = b => crypto.createHash('sha256').update(b).digest()

let mkcbor = async obj => {
  let blocks = await Promise.all(cbor.serialize(obj))
  return blocks[0]
}

const gzip = util.promisify(zlib.gzip)

let mkgzjson = async obj => {
  let buffer = await gzip(Buffer.from(JSON.stringify(obj)))
  let hash = multihashes.encode(sha2(buffer), 'sha2-256')
  let cid = new CID(1, 'raw', hash)
  return new Block(buffer, cid)
}

class GitHubArchiveGraph {
  constructor (store, root) {
    this.store = store
    this.root = root
    this._clear()
  }
  _clear () {
    this.graph = complex(this.store, this.root)
    this.graph.shardPath('/actors/*', shard)
    this.graph.shardPath('/repos/*', shard)
  }
  async put (event) {
    let actor = identifyActor(event)
    let repo = identifyRepo(event)

    let [date, time] = event.created_at.split('T')
    let [year, month, day] = date.split('-')
    time = time.replace('Z', '')
    let [hour, minute] = time.split(':')

    let b = await mkgzjson(event)
    let hash = b.cid.toBaseEncodedString()

    this.graph.add(`/actors/${actor}/${year}/${month}/${day}/${time}`, b)
    this.graph.add(`/repos/${repo}/${year}/${month}/${day}/${time}`, b)
    let k = `/timestamps/${year}/${month}/${day}/${hour}/${minute}/${hash}`
    return this.graph.add(k, b)
  }
  async _pull (filename, cid) {
    console.log({filename})
    this._eventCount = 0
    let stream = await get(`http://data.gharchive.org/${filename}`)
    let tmpf = tmp.tmpNameSync()
    stream.pipe(fs.createWriteStream(tmpf))
    await util.promisify((cb) => stream.on('end', cb))()
    stream = fs.createReadStream(tmpf)
    console.log({tmpf})
    let reader = stream.pipe(zlib.createUnzip()).pipe(JSONStream())
    let start = Date.now()
    for await (let event of reader) {
      await this.put(event)
      this._eventCount++
    }
    let block = await mkcbor({events: this._eventCount})
    this.graph.add(`/receipts/${filename}`, block)
    this._processTime = Date.now() - start
    let x = await this.graph.flush(cid)
    return x
  }
  pull (dt, continuous = false) {
    // TODO: check receipts in the current graph
    let self = this

    return (async function * () {
      let receipts = await self.graph.resolve('/receipts', self.root)
      let skip = new Set(Object.keys(receipts.value))

      while (dt < (Date.now() - fourHours)) {
        let _dt = (new Date(dt))
        let [date, time] = _dt.toISOString().split('T')
        let [year, month, day] = date.split('-')
        let [hour] = time.split(':')
        if (hour[0] === '0') hour = hour.slice(1)
        let f = `${year}-${month}-${day}-${hour}.json.gz`
        let start = Date.now()
        let graph = self.graph
        if (!skip.has(f)) {
          self.root = await self._pull(f)
          self._clear()
          let toSeconds = ms => Math.floor(ms / 1000) + 's'
          let output = {
            cid: self.root.toBaseEncodedString(),
            events: self._eventCount,
            walked: graph.nodesWalked,
            totalTime: toSeconds(Date.now() - start),
            processTime: toSeconds(self._processTime),
            graphBuildTime: toSeconds(graph._graphBuildTime)
          }
          yield output
        }

        dt += (1000 * 60 * 60)
      }
    })()
  }
}

module.exports = (...args) => new GitHubArchiveGraph(...args)
