const shard = require('./shard')
const identifyRepo = require('./identifyRepo')
const identifyActor = require('./identifyActor')

const {gzip} = require('./compression')

const CID = require('cids')

const toLink = node => {
  if (node instanceof CID) {
    return {'/': node.toBaseEncodedString()}
  } else if (node.multihash) {
    return {'/': (new CID(node.multihash)).toBaseEncodedString()}
  } else if (node.cid) {
    return {'/': node.cid.toBaseEncodedString()}
  } else {
    throw new Error('Unknown type, cannot create link.')
  }
}

const nest = (map, key, block) => {
  while (key.length > 1) {
    let _key = key.shift()
    if (!map.has(_key)) map.set(_key, new Map())
    map = map.get(_key)
  }
  map.set(key.shift(), block)
}

const _opts = { format: 'dag-cbor', hashAlg: 'sha2-256' }

class Database {
  constructor (ipfs, cid, gc = true) {
    if (!(cid instanceof CID)) cid = new CID(cid)
    this.cid = cid
    this.ipfs = ipfs
    this.root = ipfs.dag.get(cid)
    // TODO: implement garbage collection of orphaned middle nodes.
  }
  async _get (path) {
    return this.ipfs.dag.get(this.cid, path)
  }
  _realpath (path) {
    path = path.split('/').filter(x => x)
    if (!path.length) return ''
    let realpath = [path.shift()]
    if (!path.length) return [realpath[0]]
    if (realpath[0] === 'repos') {
      let owner = path.shift()
      realpath = realpath.concat(shard(owner))
      realpath.push(owner)
      if (!path.length) return realpath
      realpath.push(path.shift()) // repo name
    } else if (realpath[0] === 'actors') {
      let actor = path.shift()
      realpath = realpath.concat(shard(actor))
      realpath.push(actor)
    } else if (realpath[0] === 'receipts') {
    } else if (realpath[0] === 'timestamps') {
    } else {
      throw new Error('Invalid path.')
    }
    return realpath.concat(path)
  }
  async get (path) {
    return this._get(this._realpath(path).join('/'))
  }
  async mkblock (data) {
    let buffer = Buffer.from(JSON.stringify(data))
    let compressed = await gzip(buffer)
    return this.ipfs.block.put(compressed)
  }
  _eventKeys (event, block) {
    let repoWithOwner = identifyRepo(event)
    let actor = identifyActor(event)
    let [date, time] = event.created_at.split('T')
    let [year, month, day] = date.split('-')
    time = time.replace('Z', '')
    let [hour, minute] = time.split(':')
    let hash = block.cid.toBaseEncodedString()
    let keys = [
      `/actors/${actor}/${year}/${month}/${day}/${time}`,
      `/repos/${repoWithOwner}/${year}/${month}/${day}/${time}`,
      `/timestamps/${year}/${month}/${day}/${hour}/${minute}/${hash}`
    ]
    return keys.map(k => this._realpath(k))
  }
  async put (event) {
    let block = await this.mkblock(event)
    let keys = this._eventKeys(event, block)
    let bulk = new Map()
    keys.forEach(key => nest(bulk, key, block))
    return this._write(bulk)
  }
  async _write (bulk, orphans) {
    let _iter = async (map, node) => {
      let handleKey = async key => {
        let value = map.get(key)
        let _node
        if (value instanceof Map) {
          if (node[key]) {
            let _cid = new CID(node[key]['/'])
            if (orphans) orphans.push(_cid)
            _node = (await this.ipfs.dag.get(_cid)).value
          } else {
            _node = {}
          }
          node[key] = toLink(await _iter(value, _node))
        } else {
          node[key] = toLink(value)
        }
      }

      let keys = Array.from(map.keys())
      while (keys.length) {
        let chunk = keys.splice(0, 2)
        await Promise.all(chunk.map(k => handleKey(k)))
      }

      return this.ipfs.dag.put(node, _opts)
    }
    return _iter(bulk, (await this.root).value)
  }
}

module.exports = (...args) => new Database(...args)
module.exports.nest = nest
module.exports.toLink = toLink
