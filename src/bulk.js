const database = require('./database')

class Bulk {
  constructor (db) {
    this.db = db
    this.pending = new Map()
    this.promises = new Set()
    this._lock = false
  }
  rawWrite (key, value) {
    database.nest(this.pending, key, value)
  }
  write (event) {
    let _write = async () => {
      let block = await this.db.mkblock(event)
      let keys = this.db._eventKeys(event, block)
      keys.forEach(key => database.nest(this.pending, key, block))
    }
    let p = _write()
    this.promises.add(p)
    p.then(() => this.promises.delete(p))
    p.catch(e => {
      this.promises.delete(p)
      throw e
    })
    return p
  }
  async flush () {
    if (this._lock) throw new Error('Concurrent flush error.')
    this._lock = true
    while (this.promises.size) {
      await Promise.all(Array.from(this.promises))
    }
    let orphans = []
    let cid = await this.db._write(this.pending, orphans)
    if (cid.toBaseEncodedString() !== this.db.cid.toBaseEncodedString()) {
      orphans.push(this.db.cid)
    }
    // TODO: swap pins
    console.log('orphans', orphans.length)
    // let rm = cid => this.db.ipfs.block.rm(cid)
    while (orphans.length) {
      await Promise.all(orphans.splice(0, 2).map(c => rm(c)))
    }
    this.db = database(this.db.ipfs, cid)
    this._lock = false
    return cid
  }
}

module.exports = (...args) => new Bulk(...args)
