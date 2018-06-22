const event = require('./fixture')
const {test} = require('tap')
const database = require('../src/database')
const ipfsAPI = require('ipfs-api')
const {gunzip} = require('../src/compression')
const CID = require('cids')

const ipfs = ipfsAPI('/ip4/127.0.0.1/tcp/5001')
const _opts = { format: 'dag-cbor', hashAlg: 'sha2-256' }

test('single write', async t => {
  t.plan(7)
  let root = await ipfs.dag.put({}, _opts)
  let db = database(ipfs, root)
  let cid = await db.put(event)
  let node = await ipfs.dag.get(cid)
  t.same(Object.keys(node.value), [ 'repos', 'actors', 'timestamps' ])
  let _cid = cid.toBaseEncodedString()

  let getBlockId = async (k, subkey = '15:03:19') => {
    let key = _cid + k
    let dir = await ipfs.dag.get(key)
    let blockid = dir.value[subkey]['/']
    return blockid
  }

  let blockid = await getBlockId('/repos/24/2495/UtillYou/hope/2018/01/01')
  let block = await ipfs.block.get(blockid)
  t.same(
    await ipfs.dag.get(`${_cid}/repos/24/2495/UtillYou/hope/2018/01/01`),
    await database(ipfs, cid).get('/repos/UtillYou/hope/2018/01/01')
  )

  t.same(JSON.parse((await gunzip(block.data)).toString()), event)
  t.same(await getBlockId('/actors/24/2495/UtillYou/2018/01/01'), blockid)

  t.same(
    await ipfs.dag.get(`${_cid}/actors/24/2495/UtillYou/2018/01/01`),
    await database(ipfs, cid).get('/actors/UtillYou/2018/01/01')
  )

  let sub = (new CID(blockid)).toBaseEncodedString()
  t.same(await getBlockId('/timestamps/2018/01/01/15/03', sub), blockid)

  t.same(
    await ipfs.dag.get(`${_cid}/timestamps/2018/01/01/15/03`),
    await database(ipfs, cid).get('/timestamps/2018/01/01/15/03')
  )
})

test('write twice', async t => {
  t.plan(1)
  let root = await ipfs.dag.put({}, _opts)
  let db = database(ipfs, root)
  let cid = await db.put(event)
  let event2 = Object.assign({}, event)
  event2.repo = Object.assign({}, event2.repo)
  event2.repo.name += '2'
  db = database(ipfs, cid)
  let cid2 = await db.put(event2)
  let owner = await database(ipfs, cid2).get('/repos/UtillYou')
  t.same(Object.keys(owner.value), ['hope', 'hope2'])
})
