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

const get = bent()

const fourHours = 1000 * 60 * 60 * 4

const sha2 = b => crypto.createHash('sha256').update(b).digest()

let mkcbor = async obj => {
  return Promise.all(cbor.serialize(obj))[0]
}

let mkgzjson = obj => {
  let buffer = zlib.gzipSync(Buffer.from(JSON.stringify(obj)))
  let hash = multihashes.encode(sha2(buffer), 'sha2-256')
  let cid = new CID(1, 'raw', hash)
  return new Block(buffer, cid)
}

class GitHubArchiveGraph {
  constructor (store, root) {
    this.graph = complex(store)
    this.root = root
    this._configGraph()
  }
  _configGraph () {
    this.graph.shardPath('/actors/*', shard)
    this.graph.shardPath('/repos/*', shard)
  }
  put (event) {
    let actor = identifyActor(event)
    let repo = identifyRepo(event)

    let [date, time] = event.created_at.split('T')
    let [year, month, day] = date.split('-')
    time = time.replace('Z', '')
    let [hour, minute] = time.split(':')

    let b = mkgzjson(event)
    let hash = b.cid.toBaseEncodedString()

    this.graph.add(`/actors/${actor}/${year}/${month}/${day}/${time}`, b)
    this.graph.add(`/repos/${repo}/${year}/${month}/${day}/${time}`, b)
    let k = `/timestamps/${year}/${month}/${day}/${hour}/${minute}/${hash}`
    this.graph.add(k, b)
  }
  async _pull (filename, cid) {
    console.log({filename})
    let stream = await get(`http://data.gharchive.org/${filename}`)
    let reader = stream.pipe(zlib.createUnzip()).pipe(JSONStream())
    let events = 0
    let start = Date.now()
    for await (let event of reader) {
      await this.put(event)
      events++
    }
    let block = mkcbor({events})
    this.graph.add(`/receipts/${filename}`, block)
    this._processTime = Date.now() - start
    console.log('flush')
    return this.graph.flush(cid)
  }
  pull (dt, continuous = false) {
    // TODO: check receipts in the current graph
    let cid = this.root
    let self = this
    return (async function * () {
      while (dt < (Date.now() - fourHours)) {
        let _dt = (new Date(dt))
        let [date, time] = _dt.toISOString().split('T')
        let [year, month, day] = date.split('-')
        let [hour] = time.split(':')
        if (hour[0] === '0') hour = hour.slice(1)
        let f = `${year}-${month}-${day}-${hour}.json.gz`
        let start = Date.now()
        cid = await self._pull(f, cid)
        dt += (1000 * 60 * 60)
        let toSeconds = ms => Math.floor(ms / 1000) + 's'
        yield {
          cid: cid.toBaseEncodedString(),
          totalTime: toSeconds(Date.now() - start),
          flushTime: toSeconds(self.graph._flushTime),
          processTime: toSeconds(self._processTime),
          graphBuildTime: toSeconds(self.graph._graphBuildTime)
        }
      }
    })()
  }
}

module.exports = (...args) => new GitHubArchiveGraph(...args)
