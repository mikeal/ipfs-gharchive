const createGraph = require('./src/graph')
const createStore = require('ipld-store')
const path = require('path')

let store = createStore(path.join(__dirname, 'testdb-' + Math.random()))
let graph = createGraph(store)

const cbor = require('ipld-complex-graph-builder/fancy-cbor')(() => {})

/* serializer only works for objects with less than 500 keys */
let serialize = async obj => {
  return (await Promise.all(cbor.serialize(obj)))[0]
}
let empty = (async () => {
  let block = await serialize({})
  return block
})()

const run = async (cid) => {
  let block = await empty
  await store.put(block.cid, block.data)
  for await (let info of graph.pull(1451606400000, block.cid)) {
    console.log(info)
  }
}
run()
